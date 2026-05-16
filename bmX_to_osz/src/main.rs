use encoding_rs::SHIFT_JIS;
use rayon::prelude::*;
use crc32fast::Hasher as Crc32Hasher;
use itoa::Buffer as IntBuf;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

macro_rules! bail {
    ($($arg:tt)*) => {
        return Err(format!($($arg)*).into())
    };
}

#[derive(Clone, Copy, Debug, Default)]
struct Opts { add7k: bool, only7k: bool, addvideo: bool }

#[derive(Clone)]
struct MapTask {
    dir: PathBuf,
    charts: Vec<PathBuf>,
}

#[derive(Default, Clone)]
struct BmsMeta {
    title: String,
    artist: String,
    genre: String,
    difficulty: String,
    playlevel: String,
    rank: i32,
    bpm: f64,
    wav: Vec<Option<String>>,
    bpm_ext: Vec<Option<f64>>,
    stop_ext: Vec<Option<f64>>,
    measure_len: Vec<f64>,
}

#[derive(Clone, Copy)]
struct Note { lane: i32, t: i32, end: i32, ln: bool, wav: u16 }
#[derive(Clone, Copy)]
struct Tp { t: i32, bpm: f64 }
#[derive(Clone, Copy)]
struct AutoSample { t: i32, wav: u16 }

#[derive(Clone, Copy)]
enum EvKind { Note, Ln, Bgm, Bpm, BpmExt, Stop }
#[derive(Clone, Copy)]
struct Ev { pos: f64, kind: EvKind, ch: i32, id: u16 }

#[derive(Clone)]
struct PackItem {
    src: Option<PathBuf>,
    mem: Option<Vec<u8>>,
    name: String,
}

fn pack_push_unique(items: &mut Vec<PackItem>, seen: &mut HashSet<String>, src: PathBuf, name: String) {
    if seen.insert(name.clone()) {
        items.push(PackItem { src: Some(src), mem: None, name });
    }
}

fn pack_push_mem_unique(items: &mut Vec<PackItem>, seen: &mut HashSet<String>, data: Vec<u8>, name: String) {
    if seen.insert(name.clone()) {
        items.push(PackItem { src: None, mem: Some(data), name });
    }
}

fn od_from_rank(rank: i32) -> f64 {
    match rank { 0 => 9.9, 1 => 8.1, 2 => 7.6, 3 => 6.9, 4 => 4.9, _ => 8.0 }
}

fn first_number(s: &str) -> Option<i32> {
    let mut cur = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            cur.push(c);
        } else if !cur.is_empty() {
            break;
        }
    }
    if cur.is_empty() { None } else { cur.parse::<i32>().ok() }
}

fn build_display_version(base_name: &str, playlevel_text: &str) -> String {
    let up = base_name.to_ascii_uppercase();
    let mut level = first_number(playlevel_text).or_else(|| first_number(base_name)).unwrap_or(0);
    let (label, def_level) = if up.contains("LEGGENDARIA") { ("Leggendaria", 12) }
    else if up.contains("ANOTHER") { ("Another", 10) }
    else if up.contains("HYPER") { ("Hyper", 7) }
    else if up.contains("NORMAL") { ("Normal", 3) }
    else if up.contains("BEGINNER") { ("Beginner", 1) }
    else { ("", 0) };
    if label.is_empty() {
        let mut s = base_name.to_string();
        s.retain(|c| !matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'));
        if s.is_empty() { "Converted".to_string() } else { s }
    } else {
        if level <= 0 { level = def_level; }
        format!("{label} {level}")
    }
}

fn clean_title(title: &str) -> String {
    const TAGS: [&str; 5] = ["BEGINNER", "NORMAL", "HYPER", "ANOTHER", "LEGGENDARIA"];
    let mut t = title.trim().to_string();
    loop {
        let cur = t.trim_end().to_string();
        let mut changed = false;
        for tag in TAGS {
            // Remove suffix like " [ANOTHER]" or "(HYPER)".
            for (l, r) in [('[', ']'), ('(', ')')] {
                let mut p = String::with_capacity(tag.len() + 2);
                p.push(l);
                p.push_str(tag);
                p.push(r);
                let up = cur.to_ascii_uppercase();
                if up.ends_with(&p) {
                    let cut = cur.len().saturating_sub(p.len());
                    t = cur[..cut].trim_end().to_string();
                    changed = true;
                    break;
                }
            }
            if changed { break; }
            // Remove plain suffix like " - ANOTHER" / " ANOTHER".
            let up = cur.to_ascii_uppercase();
            let plain = format!(" {}", tag);
            let dash = format!(" - {}", tag);
            if up.ends_with(&plain) || up.ends_with(&dash) {
                let cut = if up.ends_with(&dash) { cur.len().saturating_sub(dash.len()) } else { cur.len().saturating_sub(plain.len()) };
                t = cur[..cut].trim_end().to_string();
                changed = true;
                break;
            }
        }
        if !changed { break; }
    }
    t
}

fn trim_ascii_ws(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_ascii_whitespace())
}

fn strip_prefix_ascii_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let sb = s.as_bytes();
    let pb = prefix.as_bytes();
    if sb.len() < pb.len() {
        return None;
    }
    let head = &sb[..pb.len()];
    if !head.eq_ignore_ascii_case(pb) {
        return None;
    }
    std::str::from_utf8(&sb[pb.len()..]).ok()
}

fn decode62(c: u8) -> Option<u16> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as u16),
        b'A'..=b'Z' => Some((c - b'A' + 10) as u16),
        b'a'..=b'z' => Some((c - b'a' + 36) as u16),
        _ => None,
    }
}
fn decode36(c: u8) -> Option<u16> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as u16),
        b'A'..=b'Z' => Some((c - b'A' + 10) as u16),
        b'a'..=b'z' => Some((c - b'a' + 10) as u16),
        _ => None,
    }
}
fn tok_bx(s: &[u8], base: u16) -> Option<u16> {
    if s.len() != 2 { return None; }
    match base {
        62 => Some(decode62(s[0])? * 62 + decode62(s[1])?),
        _ => Some(decode36(s[0])? * 36 + decode36(s[1])?),
    }
}

fn decode_text(bytes: &[u8]) -> String {
    let (s, _, had_err) = SHIFT_JIS.decode(bytes);
    if !had_err { return s.into_owned(); }
    String::from_utf8_lossy(bytes).into_owned()
}

fn is_chart_ext(p: &Path) -> bool {
    p.extension().and_then(|e| e.to_str()).map(|e| {
        let e = e.to_ascii_lowercase(); e == "bms" || e == "bme" || e == "bml"
    }).unwrap_or(false)
}

fn collect_chart_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for ent in fs::read_dir(dir)? {
        let p = ent?.path();
        if p.is_file() && is_chart_ext(&p) { out.push(p); }
    }
    if out.len() > 1 {
        out.sort();
    }
    Ok(out)
}

fn parse_player_line(t: &str) -> Option<i32> {
    let mut s = t.as_bytes();
    while let Some((&c, rest)) = s.split_first() {
        if c.is_ascii_whitespace() {
            s = rest;
        } else {
            break;
        }
    }
    if s.len() < 7 || !s[..7].eq_ignore_ascii_case(b"#PLAYER") {
        return None;
    }
    let mut i = 7usize;
    while i < s.len() && s[i].is_ascii_whitespace() {
        i += 1;
    }
    let start = i;
    while i < s.len() && s[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return None;
    }
    std::str::from_utf8(&s[start..i]).ok()?.parse::<i32>().ok()
}

fn collect_map_tasks(target: &Path) -> Result<Vec<MapTask>> {
    if !target.exists() || !target.is_dir() { bail!("Input folder not found: {}", target.display()); }
    let mut out = Vec::<MapTask>::new();
    // Fast startup scan: only collect chart candidates here.
    // SP filtering is done later in process_map per chart.
    let charts_root = collect_chart_files_in_dir(target)?;
    if !charts_root.is_empty() {
        out.push(MapTask { dir: target.to_path_buf(), charts: charts_root });
    }
    let mut subdirs = Vec::<PathBuf>::new();
    for ent in fs::read_dir(target)? {
        let p = ent?.path();
        if p.is_dir() {
            subdirs.push(p);
        }
    }
    let mut sub_tasks: Vec<MapTask> = subdirs
        .par_iter()
        .filter_map(|p| {
            match collect_chart_files_in_dir(p) {
                Ok(charts) if !charts.is_empty() => Some(MapTask { dir: p.clone(), charts }),
                _ => None,
            }
        })
        .collect();
    out.append(&mut sub_tasks);
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    Ok(out)
}

fn parse_opts(args: &[OsString]) -> Result<(PathBuf, Opts)> {
    if args.len() < 2 { bail!("Usage: bmX_to_osz.exe <map_folder OR root_folder_with_map_subfolders> [-add7k|-only7k] [-addvideo]"); }
    let mut o = Opts::default();
    for f in &args[2..] {
        match f.to_string_lossy().to_ascii_lowercase().as_str() {
            "-add7k" => o.add7k = true,
            "-only7k" => o.only7k = true,
            "-addvideo" => o.addvideo = true,
            _ => bail!("Unknown flag: {}", f.to_string_lossy()),
        }
    }
    if o.add7k && o.only7k { bail!("Flags -add7k and -only7k cannot be used together."); }
    Ok((PathBuf::from(&args[1]), o))
}

fn lane_from_ch(ch: i32, cut_scratch: bool, use_iidx_layout: bool) -> Option<i32> {
    let hi = ch / 10;
    let lo = ch % 10;
    if !(1..=9).contains(&lo) { return None; }
    if hi == 1 {
        if use_iidx_layout {
            let lane = match lo {
                1 => if cut_scratch { 0 } else { 1 },
                2 => if cut_scratch { 1 } else { 2 },
                3 => if cut_scratch { 2 } else { 3 },
                4 => if cut_scratch { 3 } else { 4 },
                5 => if cut_scratch { 4 } else { 5 },
                6 => if cut_scratch { -1 } else { 0 },
                8 => if cut_scratch { 5 } else { 6 },
                9 => if cut_scratch { 6 } else { 7 },
                _ => -1,
            };
            return if lane >= 0 { Some(lane) } else { None };
        }
        return Some(lo - 1);
    }
    if hi == 2 {
        return Some((if use_iidx_layout { 8 } else { 9 }) + (lo - 1));
    }
    if hi == 5 {
        if use_iidx_layout {
            let lane = match lo {
                1 => if cut_scratch { 0 } else { 1 },
                2 => if cut_scratch { 1 } else { 2 },
                3 => if cut_scratch { 2 } else { 3 },
                4 => if cut_scratch { 3 } else { 4 },
                5 => if cut_scratch { 4 } else { 5 },
                6 => if cut_scratch { -1 } else { 0 },
                8 => if cut_scratch { 5 } else { 6 },
                9 => if cut_scratch { 6 } else { 7 },
                _ => -1,
            };
            return if lane >= 0 { Some(lane) } else { None };
        }
        return Some(lo - 1);
    }
    if hi == 6 {
        return Some((if use_iidx_layout { 8 } else { 9 }) + (lo - 1));
    }
    None
}

fn dedupe_timing_points(tp: &mut Vec<Tp>) {
    if tp.len() <= 1 {
        return;
    }
    tp.sort_by(|a, b| a.t.cmp(&b.t).then_with(|| a.bpm.partial_cmp(&b.bpm).unwrap_or(std::cmp::Ordering::Equal)));
    let mut out = Vec::<Tp>::with_capacity(tp.len());
    for cur in tp.iter().copied() {
        if out.is_empty() {
            out.push(cur);
            continue;
        }
        let prev = out[out.len() - 1];
        if prev.t == cur.t && (prev.bpm - cur.bpm).abs() < 1e-7 {
            continue;
        }
        if (prev.bpm - cur.bpm).abs() < 1e-7 {
            continue;
        }
        out.push(cur);
    }
    *tp = out;
}

fn fmt_g15(v: f64) -> String {
    let s = format!("{:.14e}", v);
    let (mant, exp) = match s.split_once('e') {
        Some(x) => x,
        None => return s,
    };
    let exp = exp.parse::<i32>().unwrap_or(0);
    let mut digits = mant.replace('.', "");
    if exp >= 0 {
        let shift = exp as usize;
        if shift + 1 >= digits.len() {
            digits.push_str(&"0".repeat(shift + 1 - digits.len()));
            return digits;
        }
        let int = digits[..=shift].to_string();
        let frac = digits[shift + 1..].trim_end_matches('0').to_string();
        if frac.is_empty() { int } else { format!("{int}.{frac}") }
    } else {
        let zeros = "0".repeat((-exp - 1) as usize);
        digits = digits.trim_start_matches('0').to_string();
        if digits.is_empty() { return String::from("0"); }
        format!("0.{}{}", zeros, digits.trim_end_matches('0'))
    }
}

fn parse_bms_from_text(path: &Path, text: &str, cut_scratch: bool) -> Result<(bool, bool, BmsMeta, Vec<Note>, Vec<Tp>, Vec<AutoSample>)> {
    let mut m = BmsMeta { bpm: 130.0, rank: -1, ..Default::default() };
    m.wav.resize(2048, None);
    m.bpm_ext.resize(2048, None);
    m.stop_ext.resize(2048, None);
    m.measure_len.resize(256, 1.0);
    let mut ev = Vec::<Ev>::new();
    let mut token_base: u16 = 36;
    let (mut has16, mut has17, mut has1819) = (false, false, false);
    let mut player = None;
    let (mut has11, mut has21) = (false, false);

    let tb = text.as_bytes();
    let mut start = 0usize;
    for i in 0..=tb.len() {
        if i != tb.len() && tb[i] != b'\n' {
            continue;
        }
        let mut line = &tb[start..i];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len().saturating_sub(1)];
        }
        start = i.saturating_add(1);
        while let Some((&c, rest)) = line.split_first() {
            if c.is_ascii_whitespace() {
                line = rest;
            } else {
                break;
            }
        }
        while line.last().map(|c| c.is_ascii_whitespace()).unwrap_or(false) {
            line = &line[..line.len().saturating_sub(1)];
        }
        if line.is_empty() || line[0] != b'#' {
            continue;
        }
        let Ok(t) = std::str::from_utf8(line) else { continue; };
        let b = t.as_bytes();
        if player.is_none() {
            player = parse_player_line(t);
        }
        if let Some(v) = strip_prefix_ascii_case(t, "#TITLE ") { m.title = v.trim().to_string(); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#ARTIST ") { m.artist = v.trim().to_string(); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#GENRE ") { m.genre = v.trim().to_string(); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#DIFFICULTY ") { m.difficulty = v.trim().to_string(); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#PLAYLEVEL ") { m.playlevel = v.trim().to_string(); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#RANK ") { m.rank = v.trim().parse().unwrap_or(-1); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#BPM ") { m.bpm = v.trim().parse().unwrap_or(m.bpm); continue; }
        if let Some(v) = strip_prefix_ascii_case(t, "#BASE ") {
            let n = v.trim().parse::<u16>().unwrap_or(36);
            token_base = if n == 62 { 62 } else { 36 };
            continue;
        }

        if b.len() >= 7 && b[0] == b'#' && b[1].is_ascii_digit() && b[2].is_ascii_digit() && b[3].is_ascii_digit() && b[4] == b'0' && b[5] == b'2' && b[6] == b':' {
            let mnum = t[1..4].parse::<i32>().unwrap_or(0);
            if let Ok(v) = t[7..].trim().parse::<f64>() {
                if v > 0.0 && mnum >= 0 {
                    let mi = mnum as usize;
                    if mi >= m.measure_len.len() {
                        m.measure_len.resize(mi + 1, 1.0);
                    }
                    m.measure_len[mi] = v;
                }
            }
            continue;
        }
        if t.len() >= 8 && t[..4].eq_ignore_ascii_case("#WAV") {
            if let Some(id) = tok_bx(&t.as_bytes()[4..6], token_base) {
                let val = t[6..].trim().to_string();
                if !val.is_empty() {
                    let idx = id as usize;
                    if idx >= m.wav.len() {
                        m.wav.resize(idx + 1, None);
                    }
                    m.wav[idx] = Some(val);
                }
            }
            continue;
        }
        if t.len() >= 8 && t[..4].eq_ignore_ascii_case("#BPM") && !t.as_bytes()[4].is_ascii_whitespace() {
            if let Some(id) = tok_bx(&t.as_bytes()[4..6], token_base) {
                if let Ok(v) = t[6..].trim().parse::<f64>() {
                    let idx = id as usize;
                    if idx >= m.bpm_ext.len() {
                        m.bpm_ext.resize(idx + 1, None);
                    }
                    m.bpm_ext[idx] = Some(v);
                }
            }
            continue;
        }
        if t.len() >= 9 && t[..5].eq_ignore_ascii_case("#STOP") {
            if let Some(id) = tok_bx(&t.as_bytes()[5..7], token_base) {
                if let Ok(v) = t[7..].trim().parse::<f64>() {
                    let idx = id as usize;
                    if idx >= m.stop_ext.len() {
                        m.stop_ext.resize(idx + 1, None);
                    }
                    m.stop_ext[idx] = Some(v);
                }
            }
            continue;
        }

        let b = t.as_bytes();
        if b.len() < 8 || b[0] != b'#' || b[6] != b':' { continue; }
        let measure = t[1..4].parse::<i32>().unwrap_or(0);
        let ch = if t.as_bytes()[4].is_ascii_digit() && t.as_bytes()[5].is_ascii_digit() {
            ((t.as_bytes()[4] - b'0') as i32) * 10 + (t.as_bytes()[5] - b'0') as i32
        } else {
            -1
        };
        if (0x11..=0x19).contains(&ch) { has11 = true; }
        if (0x21..=0x29).contains(&ch) { has21 = true; }
        let data = t[7..].trim().as_bytes();
        if data.len() < 2 || data.len() % 2 != 0 { continue; }
        let slots = data.len() / 2;
        for i in 0..slots {
            let tok = &data[i * 2..i * 2 + 2];
            if tok == b"00" { continue; }
            let Some(id) = tok_bx(tok, token_base) else { continue; };
            let pos = measure as f64 + (i as f64 / slots as f64);
            let hi = ch / 10;
            let lo = ch % 10;
            if hi == 1 || hi == 5 {
                if lo == 6 { has16 = true; }
                if lo == 7 { has17 = true; }
                if lo == 8 || lo == 9 { has1819 = true; }
            }
            match ch {
                0x03 => {
                    if let Ok(hex) = std::str::from_utf8(tok) {
                        if let Ok(v) = i32::from_str_radix(hex, 16) {
                            if v > 0 {
                                ev.push(Ev { pos, kind: EvKind::Bpm, ch, id: v as u16 });
                            }
                        }
                    }
                }
                0x08 => ev.push(Ev { pos, kind: EvKind::BpmExt, ch, id }),
                0x09 => ev.push(Ev { pos, kind: EvKind::Stop, ch, id }),
                0x01 => ev.push(Ev { pos, kind: EvKind::Bgm, ch, id }),
                51..=59 | 61..=69 => ev.push(Ev { pos, kind: EvKind::Ln, ch, id }),
                _ => {
                    if (11..=19).contains(&ch) || (21..=29).contains(&ch) || (51..=59).contains(&ch) || (61..=69).contains(&ch) {
                        ev.push(Ev { pos, kind: EvKind::Note, ch, id });
                    }
                }
            }
        }
    }
    let use_iidx_layout = has16 && has1819 && !has17;
    let is_sp = if player == Some(1) {
        true
    } else if player.is_none() {
        has11 && !has21
    } else {
        false
    };
    let is_5plus1 = is_sp && has16 && !has1819;

    if m.title.is_empty() { m.title = path.file_stem().and_then(|s| s.to_str()).unwrap_or("Unknown").to_string(); }
    if m.artist.is_empty() { m.artist = "Unknown Artist".to_string(); }

    let mut max_measure = 0i32;
    for e in &ev {
        let mnum = e.pos.floor() as i32;
        if mnum > max_measure { max_measure = mnum; }
    }
    if !m.measure_len.is_empty() {
        let last = (m.measure_len.len() - 1) as i32;
        if last > max_measure {
            max_measure = last;
        }
    }
    let mut prefix = vec![0.0f64; (max_measure.max(0) as usize) + 2];
    for i in 0..=max_measure.max(0) as usize {
        let ml = if i < m.measure_len.len() { m.measure_len[i] } else { 1.0 };
        prefix[i + 1] = prefix[i] + 4.0 * ml;
    }
    for e in &mut ev {
        let mnum = e.pos.floor() as i32;
        let frac = e.pos - mnum as f64;
        let ml = if mnum >= 0 {
            let mi = mnum as usize;
            if mi < m.measure_len.len() { m.measure_len[mi] } else { 1.0 }
        } else {
            1.0
        };
        e.pos = prefix[mnum.max(0) as usize] + frac * (4.0 * ml);
    }
    if ev.len() > 1 {
        ev.sort_by(|a, b| {
            a.pos.partial_cmp(&b.pos)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.ch.cmp(&b.ch))
        });
    }

    let mut bpm = if m.bpm > 0.0 { m.bpm } else { 130.0 };
    let mut beats_cur = 0.0;
    let mut ms_cur = 0.0;
    let mut tps = vec![Tp { t: 0, bpm }];
    let mut notes = Vec::<Note>::new();
    let mut autos = Vec::<AutoSample>::new();
    let mut ln_open: [Option<(i32, u16)>; 20] = [None; 20];

    for e in ev {
        let beats_abs = e.pos;
        let beats = beats_abs - beats_cur;
        ms_cur += beats * 60000.0 / bpm;
        beats_cur = beats_abs;
        let now = ms_cur.round() as i32;

        match e.kind {
            EvKind::Bpm => {
                bpm = e.id as f64;
                tps.push(Tp { t: now, bpm });
            }
            EvKind::BpmExt => {
                if let Some(Some(v)) = m.bpm_ext.get(e.id as usize) {
                    if *v > 0.0 {
                        bpm = *v;
                        tps.push(Tp { t: now, bpm });
                    }
                }
            }
            EvKind::Stop => {
                if let Some(Some(v)) = m.stop_ext.get(e.id as usize) {
                    ms_cur += (60000.0 / bpm) * (*v / 48.0);
                }
            }
            EvKind::Bgm => autos.push(AutoSample { t: now, wav: e.id }),
            EvKind::Note => {
                let hi = e.ch / 10;
                let lo = e.ch % 10;
                if cut_scratch && use_iidx_layout && (hi == 1 || hi == 2 || hi == 5 || hi == 6) && lo == 6 {
                    if e.id > 0 {
                        autos.push(AutoSample { t: now, wav: e.id });
                    }
                    continue;
                }
                if let Some(lane) = lane_from_ch(e.ch, cut_scratch, use_iidx_layout) {
                    if lane >= 0 {
                        notes.push(Note { lane, t: now, end: now, ln: false, wav: e.id });
                    }
                }
            }
            EvKind::Ln => {
                let hi = e.ch / 10;
                let lo = e.ch % 10;
                if cut_scratch && use_iidx_layout && (hi == 1 || hi == 2 || hi == 5 || hi == 6) && lo == 6 {
                    if e.id > 0 {
                        autos.push(AutoSample { t: now, wav: e.id });
                    }
                    continue;
                }
                if let Some(lane) = lane_from_ch(e.ch, cut_scratch, use_iidx_layout) {
                    if lane < 0 { continue; }
                    let li = lane as usize;
                    if li >= ln_open.len() {
                        continue;
                    }
                    if let Some((start, swav)) = ln_open[li].take() {
                        let mut end = now;
                        if end <= start {
                            end = start + 1;
                        }
                        notes.push(Note { lane, t: start, end, ln: true, wav: swav });
                        if e.id > 0 {
                            autos.push(AutoSample { t: end, wav: e.id });
                        }
                    } else {
                        ln_open[li] = Some((now, e.id));
                    }
                }
            }
        }
    }

    for (lane, v) in ln_open.iter().enumerate() {
        if let Some((start, swav)) = *v {
            notes.push(Note { lane: lane as i32, t: start, end: start + 120, ln: true, wav: swav });
        }
    }

    if notes.len() > 1 {
        notes.sort_by(|a, b| a.t.cmp(&b.t).then(a.lane.cmp(&b.lane)).then((a.ln as i32).cmp(&(b.ln as i32))));
    }
    dedupe_timing_points(&mut tps);
    if autos.len() > 1 {
        autos.sort_by(|a, b| a.t.cmp(&b.t).then(a.wav.cmp(&b.wav)));
    }
    let mut autos_dedup = Vec::<AutoSample>::with_capacity(autos.len());
    for a in autos {
        if let Some(p) = autos_dedup.last() {
            if p.wav == a.wav && (p.t - a.t).abs() <= 1 {
                continue;
            }
        }
        autos_dedup.push(a);
    }
    autos = autos_dedup;
    Ok((is_sp, is_5plus1, m, notes, tps, autos))
}

fn build_osu_bytes(meta: &BmsMeta, notes: &[Note], tp: &[Tp], autos: &[AutoSample], keys: i32, bg_name: &str, video_name: &str, display_version: &str) -> Result<Vec<u8>> {
    let mut f = Vec::<u8>::with_capacity(256 * 1024);
    let wav_short: Vec<Option<&str>> = meta
        .wav
        .iter()
        .map(|o| {
            o.as_ref().map(|v| {
                Path::new(v)
                    .file_name()
                    .and_then(|x| x.to_str())
                    .unwrap_or(v.as_str())
            })
        })
        .collect();
    writeln!(f, "osu file format v14\r")?;
    writeln!(f, "\r")?;
    writeln!(f, "[General]\r")?;
    writeln!(f, "Mode:3\r")?;
    writeln!(f, "SampleSet:Soft\r")?;
    writeln!(f, "Countdown:0\r")?;
    if keys == 8 { writeln!(f, "SpecialStyle:1\r")?; }

    writeln!(f, "\r")?;
    writeln!(f, "[Editor]\r")?;
    writeln!(f, "DistanceSpacing:1\r")?;
    writeln!(f, "BeatDivisor:1\r")?;
    writeln!(f, "GridSize:1\r")?;
    writeln!(f, "TimelineZoom:1\r")?;

    writeln!(f, "\r")?;
    writeln!(f, "[Metadata]\r")?;
    let clean = clean_title(&meta.title);
    writeln!(f, "Title:{}\r", clean)?;
    writeln!(f, "TitleUnicode:{}\r", clean)?;
    writeln!(f, "Artist:{}\r", meta.artist)?;
    writeln!(f, "ArtistUnicode:{}\r", meta.artist)?;
    writeln!(f, "Creator:bmX_to_osz\r")?;
    writeln!(f, "Version:{}\r", display_version)?;
    writeln!(f, "Source:\r")?;
    writeln!(f, "Tags:{}\r", meta.genre)?;
    writeln!(f, "BeatmapID:0\r")?;
    writeln!(f, "BeatmapSetID:0\r")?;

    writeln!(f, "\r")?;
    writeln!(f, "[Difficulty]\r")?;
    writeln!(f, "HPDrainRate:7.5\r")?;
    writeln!(f, "CircleSize:{}\r", keys)?;
    writeln!(f, "OverallDifficulty:{:.2}\r", od_from_rank(meta.rank))?;
    writeln!(f, "ApproachRate:0\r")?;
    writeln!(f, "SliderMultiplier:1\r")?;
    writeln!(f, "SliderTickRate:1\r")?;

    writeln!(f, "\r")?;
    writeln!(f, "[Events]\r")?;
    writeln!(f, "0,0,\"{}\",0,0\r", bg_name)?;
    if !video_name.is_empty() {
        writeln!(f, "Video,0,\"{}\"\r", video_name)?;
    }
    for a in autos {
        if let Some(Some(s)) = wav_short.get(a.wav as usize) {
            writeln!(f, "Sample,{},0,\"{}\",100\r", a.t, s)?;
        }
    }

    writeln!(f, "\r")?;
    writeln!(f, "[TimingPoints]\r")?;
    let mut ibuf = IntBuf::new();
    for t in tp {
        f.extend_from_slice(ibuf.format(t.t).as_bytes());
        f.push(b',');
        f.extend_from_slice(fmt_g15(60000.0 / t.bpm).as_bytes());
        f.extend_from_slice(b",4,2,0,0,1,0\r\n");
    }

    writeln!(f, "\r")?;
    writeln!(f, "[HitObjects]\r")?;
    for n in notes {
        let x = ((512.0 / keys as f64) * n.lane as f64 + (256.0 / keys as f64)).floor() as i32;
        let sample = wav_short
            .get(n.wav as usize)
            .and_then(|v| *v)
            .unwrap_or("");
        if !n.ln {
            f.extend_from_slice(ibuf.format(x).as_bytes());
            f.extend_from_slice(b",192,");
            f.extend_from_slice(ibuf.format(n.t).as_bytes());
            if sample.is_empty() {
                f.extend_from_slice(b",1,0,0:0:0:0:\r\n");
            } else {
                f.extend_from_slice(b",1,0,0:0:0:100:");
                f.extend_from_slice(sample.as_bytes());
                f.extend_from_slice(b"\r\n");
            }
        } else {
            f.extend_from_slice(ibuf.format(x).as_bytes());
            f.extend_from_slice(b",192,");
            f.extend_from_slice(ibuf.format(n.t).as_bytes());
            f.extend_from_slice(b",128,0,");
            f.extend_from_slice(ibuf.format(n.end).as_bytes());
            if sample.is_empty() {
                f.extend_from_slice(b":0:0:0:0:\r\n");
            } else {
                f.extend_from_slice(b":0:0:0:100:");
                f.extend_from_slice(sample.as_bytes());
                f.extend_from_slice(b"\r\n");
            }
        }
    }
    Ok(f)
}

fn collect_bg_image_names(exe_dir: &Path) -> Result<Vec<String>> {
    let bg_dir = exe_dir.join("bg");
    let mut names = Vec::<String>::new();
    if bg_dir.is_dir() {
        for ent in fs::read_dir(&bg_dir)? {
            let p = ent?.path();
            if !p.is_file() { continue; }
            let name = p.file_name().map(|x| x.to_string_lossy().into_owned()).unwrap_or_default();
            if name.is_empty() { continue; }
            let low = name.to_ascii_lowercase();
            if low.ends_with(".png") || low.ends_with(".jpg") || low.ends_with(".jpeg") || low.ends_with(".bmp") || low.ends_with(".webp") {
                names.push(name);
            }
        }
    }
    names.sort();
    Ok(names)
}

fn pick_bg_name(map_dir: &Path, bg_names: &[String]) -> String {
    if !bg_names.is_empty() {
        let key = map_dir.to_string_lossy();
        let mut h: u32 = 2166136261;
        for b in key.as_bytes() {
            h ^= *b as u32;
            h = h.wrapping_mul(16777619);
        }
        let idx = (h as usize) % bg_names.len();
        return bg_names[idx].clone();
    }
    String::from("1.png")
}

fn collect_assets(map_dir: &Path, exe_dir: &Path, include_video: bool, bg_names: &[String]) -> Result<(String, String, Vec<PackItem>)> {
    let mut bg = String::from("1.png");
    let mut first_video = String::new();
    let mut first_video_path = PathBuf::new();
    // Front-load capacity to reduce reallocations on large audio folders.
    let mut pack = Vec::<PackItem>::with_capacity(2048);
    let mut seen = HashSet::<String>::with_capacity(4096);
    let mut stack = vec![map_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for ent in fs::read_dir(&dir)? {
            let p = ent?.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if !p.is_file() {
                continue;
            }
        let name = p.file_name().map(|x| x.to_string_lossy().into_owned()).unwrap_or_default();
        if name.is_empty() { continue; }
        if name.eq_ignore_ascii_case("1.png") && p.parent() == Some(map_dir) {
            bg = name.clone();
        }
        let ext = p.extension().and_then(|x| x.to_str()).unwrap_or("");
        let is_video = ext.eq_ignore_ascii_case("mp4")
            || ext.eq_ignore_ascii_case("avi")
            || ext.eq_ignore_ascii_case("mkv")
            || ext.eq_ignore_ascii_case("webm")
            || ext.eq_ignore_ascii_case("mov");
        if include_video && first_video.is_empty() && is_video {
            if let Ok(md) = fs::metadata(&p) {
                if md.len() >= 5 * 1024 * 1024 {
                    first_video = name.clone();
                    first_video_path = p.clone();
                }
            }
        }
        let is_audio = ext.eq_ignore_ascii_case("wav")
            || ext.eq_ignore_ascii_case("ogg")
            || ext.eq_ignore_ascii_case("mp3");
        if is_audio && !name.eq_ignore_ascii_case("preview_auto_generator.wav") {
            pack_push_unique(&mut pack, &mut seen, p, name);
        }
        }
    }
    let has_external_bg_pool = !bg_names.is_empty();
    if !has_external_bg_pool {
        let map_bg = map_dir.join("1.png");
        if map_bg.is_file() {
            pack_push_unique(&mut pack, &mut seen, map_bg, String::from("1.png"));
        }
    }
    if include_video && !first_video.is_empty() && first_video_path.is_file() {
        pack_push_unique(&mut pack, &mut seen, first_video_path, first_video.clone());
    }
    let external_bg = pick_bg_name(map_dir, bg_names);
    if external_bg != "1.png" {
        let src = exe_dir.join("bg").join(&external_bg);
        if src.is_file() {
            pack_push_unique(&mut pack, &mut seen, src, external_bg.clone());
            bg = external_bg;
        }
    }
    Ok((bg, first_video, pack))
}

fn build_osz_store(out: &Path, files: &[PackItem]) -> Result<()> {
    let mut z = BufWriter::with_capacity(1024 * 1024, fs::File::create(out)?);
    let mut central: Vec<(Vec<u8>, u32, u32, u32)> = Vec::with_capacity(files.len());
    let mut buf = vec![0u8; 1024 * 1024];
    let mut cur_off: u32 = 0;
    for p in files {
        let name = p.name.as_bytes().to_vec();
        let mut hasher = Crc32Hasher::new();
        let mut sz: u32 = 0;
        let off = cur_off;

        let mut local_hdr = [0u8; 30];
        local_hdr[0..4].copy_from_slice(&0x04034B50u32.to_le_bytes());
        local_hdr[4..6].copy_from_slice(&20u16.to_le_bytes());
        local_hdr[6..8].copy_from_slice(&0x0008u16.to_le_bytes());
        local_hdr[26..28].copy_from_slice(&(name.len() as u16).to_le_bytes());
        z.write_all(&local_hdr)?;
        z.write_all(&name)?;
        cur_off = cur_off.saturating_add((local_hdr.len() + name.len()) as u32);

        if let Some(src) = &p.src {
            let mut input = BufReader::with_capacity(1024 * 1024, fs::File::open(src)?);
            loop {
                let n = input.read(&mut buf)?;
                if n == 0 { break; }
                let chunk = &buf[..n];
                hasher.update(chunk);
                sz = sz.saturating_add(n as u32);
                z.write_all(chunk)?;
            }
        } else if let Some(mem) = &p.mem {
            hasher.update(mem);
            sz = mem.len() as u32;
            z.write_all(mem)?;
        }
        cur_off = cur_off.saturating_add(sz);

        let crc = hasher.finalize();
        let mut dd = [0u8; 16];
        dd[0..4].copy_from_slice(&0x08074B50u32.to_le_bytes());
        dd[4..8].copy_from_slice(&crc.to_le_bytes());
        dd[8..12].copy_from_slice(&sz.to_le_bytes());
        dd[12..16].copy_from_slice(&sz.to_le_bytes());
        z.write_all(&dd)?;
        cur_off = cur_off.saturating_add(dd.len() as u32);
        central.push((name, crc, sz, off));
    }
    let cd_off = cur_off;
    for (name, crc, sz, off) in &central {
        let mut cd = [0u8; 46];
        cd[0..4].copy_from_slice(&0x02014B50u32.to_le_bytes());
        cd[4..6].copy_from_slice(&20u16.to_le_bytes());
        cd[6..8].copy_from_slice(&20u16.to_le_bytes());
        cd[8..10].copy_from_slice(&0x0008u16.to_le_bytes());
        cd[16..20].copy_from_slice(&crc.to_le_bytes());
        cd[20..24].copy_from_slice(&sz.to_le_bytes());
        cd[24..28].copy_from_slice(&sz.to_le_bytes());
        cd[28..30].copy_from_slice(&(name.len() as u16).to_le_bytes());
        cd[42..46].copy_from_slice(&off.to_le_bytes());
        z.write_all(&cd)?;
        z.write_all(name)?;
        cur_off = cur_off.saturating_add((cd.len() + name.len()) as u32);
    }
    let cd_end = cur_off;
    let mut eocd = [0u8; 22];
    eocd[0..4].copy_from_slice(&0x06054B50u32.to_le_bytes());
    eocd[4..6].copy_from_slice(&0u16.to_le_bytes());
    eocd[6..8].copy_from_slice(&0u16.to_le_bytes());
    eocd[8..10].copy_from_slice(&(central.len() as u16).to_le_bytes());
    eocd[10..12].copy_from_slice(&(central.len() as u16).to_le_bytes());
    eocd[12..16].copy_from_slice(&(cd_end - cd_off).to_le_bytes());
    eocd[16..20].copy_from_slice(&cd_off.to_le_bytes());
    eocd[20..22].copy_from_slice(&0u16.to_le_bytes());
    z.write_all(&eocd)?;
    Ok(())
}

fn process_map(task: &MapTask, out_dir: &Path, exe_dir: &Path, opts: Opts, bg_names: &[String]) -> Result<()> {
    if task.charts.is_empty() { return Ok(()); }
    let map_dir = &task.dir;
    let map_name = map_dir.file_name().and_then(|s| s.to_str()).unwrap_or("map").to_string();
    let (bg_name, video_name, mut pack_items) = collect_assets(map_dir, exe_dir, opts.addvideo, bg_names)?;
    let vid = if opts.addvideo { video_name.clone() } else { String::new() };
    let parse_chart = |chart: &PathBuf| -> Result<Vec<PackItem>> {
            let mut out = Vec::<PackItem>::new();
            let text = decode_text(&fs::read(chart)?);
            let raw_stem = chart.file_stem().and_then(|s| s.to_str()).unwrap_or("chart");
            let stem = trim_ascii_ws(raw_stem);
            if stem.is_empty() {
                return Ok(out);
            }
            if !opts.only7k {
                let (is_sp, is_5plus1, meta, notes8, tp, autos) = parse_bms_from_text(chart, &text, false)?;
                if !is_sp {
                    return Ok(out);
                }
                if is_5plus1 {
                    return Ok(out);
                }
                if !notes8.is_empty() {
                    let ver = build_display_version(stem, &meta.playlevel);
                    let osu_data = build_osu_bytes(&meta, &notes8, &tp, &autos, 8, &bg_name, &vid, &ver)?;
                    out.push(PackItem { src: None, mem: Some(osu_data), name: format!("{}.osu", stem) });
                }
            }
            if opts.add7k || opts.only7k {
                let (is_sp, is_5plus1, meta, notes7, tp, autos) = parse_bms_from_text(chart, &text, true)?;
                if !is_sp {
                    return Ok(out);
                }
                if is_5plus1 {
                    return Ok(out);
                }
                if !notes7.is_empty() {
                    let ver = build_display_version(stem, &meta.playlevel);
                    let osu_data = build_osu_bytes(&meta, &notes7, &tp, &autos, 7, &bg_name, &vid, &ver)?;
                    out.push(PackItem { src: None, mem: Some(osu_data), name: format!("{}_7k.osu", stem) });
                }
            }
            Ok(out)
    };
    // For small chart counts (common single-map case), rayon startup/scheduling overhead
    // tends to cost more than it saves.
    let chart_jobs: Result<Vec<Vec<PackItem>>> = if task.charts.len() <= 3 {
        // Single-map fast path: avoid rayon/sync overhead entirely.
        let mut v = Vec::with_capacity(task.charts.len());
        for c in &task.charts {
            v.push(parse_chart(c)?);
        }
        Ok(v)
    } else {
        task.charts.par_iter().map(parse_chart).collect()
    };

    let mut pack_seen: HashSet<String> = pack_items.iter().map(|p| p.name.clone()).collect();
    for set in chart_jobs? {
        for item in set {
            if let Some(data) = item.mem {
                pack_push_mem_unique(&mut pack_items, &mut pack_seen, data, item.name);
            }
        }
    }
    let has_osu = pack_items.iter().any(|p| p.name.to_ascii_lowercase().ends_with(".osu"));
    if !has_osu {
        println!("Skipped (no eligible charts): {}", map_dir.display());
        return Ok(());
    }

    let osz = out_dir.join(format!("{}.osz", map_name));
    // Keep insertion order to avoid extra O(n log n) work on large audio packs.
    build_osz_store(&osz, &pack_items)?;
    println!("Created: {}", osz.display());
    Ok(())
}

fn main() -> Result<()> {
    let args_os: Vec<OsString> = env::args_os().collect();

    let t0 = Instant::now();
    let (target, opts) = parse_opts(&args_os)?;
    println!("Starting: {}", target.display());
    let root_charts = if target.is_dir() {
        collect_chart_files_in_dir(&target)?
    } else {
        Vec::new()
    };
    let map_tasks = if !root_charts.is_empty() {
        vec![MapTask { dir: target.clone(), charts: root_charts }]
    } else {
        collect_map_tasks(&target)?
    };
    if map_tasks.is_empty() { bail!("No SP chart folders with .bme/.bms found in: {}", target.display()); }
    println!("MapsFound: {}", map_tasks.len());

    let exe_dir = env::current_exe()?.parent().unwrap().to_path_buf();
    let out_dir = exe_dir.join("output");
    fs::create_dir_all(&out_dir)?;
    let bg_names = collect_bg_image_names(&exe_dir)?;

    let use_parallel = map_tasks.len() >= 3;
    if use_parallel {
        map_tasks.par_iter().for_each(|m| {
            if let Err(e) = process_map(m, &out_dir, &exe_dir, opts, &bg_names) {
                eprintln!("Failed {}: {e}", m.dir.display());
            }
        });
    } else {
        for m in &map_tasks {
            // quiet hot path logging
            if let Err(e) = process_map(m, &out_dir, &exe_dir, opts, &bg_names) {
                eprintln!("Failed {}: {e}", m.dir.display());
            }
        }
    }
    println!("ExecutionTimeMs: {}", t0.elapsed().as_millis());
    Ok(())
}
