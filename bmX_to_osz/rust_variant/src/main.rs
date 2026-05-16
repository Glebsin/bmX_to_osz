use encoding_rs::SHIFT_JIS;
use rayon::prelude::*;
use crc32fast::Hasher as Crc32Hasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
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
    wav: HashMap<u16, String>,
    bpm_ext: HashMap<u16, f64>,
    stop_ext: HashMap<u16, f64>,
    measure_len: HashMap<i32, f64>,
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
    let key = name.to_ascii_lowercase();
    if seen.insert(key) {
        items.push(PackItem { src: Some(src), mem: None, name });
    }
}

fn pack_push_mem_unique(items: &mut Vec<PackItem>, seen: &mut HashSet<String>, data: Vec<u8>, name: String) {
    let key = name.to_ascii_lowercase();
    if seen.insert(key) {
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

fn trim_ascii_ws(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_ascii_whitespace())
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
        let e = e.to_ascii_lowercase(); e == "bms" || e == "bme"
    }).unwrap_or(false)
}

fn collect_chart_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for ent in fs::read_dir(dir)? {
        let p = ent?.path();
        if p.is_file() && is_chart_ext(&p) { out.push(p); }
    }
    out.sort();
    Ok(out)
}

fn has_chart_ext_in_dir(dir: &Path) -> Result<bool> {
    for ent in fs::read_dir(dir)? {
        let p = ent?.path();
        if p.is_file() && is_chart_ext(&p) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn collect_sp_chart_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for p in collect_chart_files_in_dir(dir)? {
        if is_sp_chart_file(&p)? {
            out.push(p);
        }
    }
    Ok(out)
}

fn parse_channel_from_line(line: &str) -> Option<i32> {
    let b = line.as_bytes();
    if b.len() < 7 || b[0] != b'#' || b[6] != b':' { return None; }
    if !(b[1].is_ascii_digit() && b[2].is_ascii_digit() && b[3].is_ascii_digit()) { return None; }
    if !(b[4].is_ascii_digit() && b[5].is_ascii_digit()) { return None; }
    Some(((b[4] - b'0') as i32) * 10 + (b[5] - b'0') as i32)
}

fn parse_player_line(t: &str) -> Option<i32> {
    let s = t.trim_start();
    if s.len() < 7 || !s[..7].eq_ignore_ascii_case("#PLAYER") {
        return None;
    }
    let rest = s[7..].trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() { None } else { digits.parse::<i32>().ok() }
}

fn is_sp_chart_text(text: &str) -> bool {
    let mut player = None;
    let (mut has11, mut has21) = (false, false);
    for raw in text.lines() {
        let t = raw.trim();
        if t.is_empty() { continue; }
        if player.is_none() {
            player = parse_player_line(t);
        }
        if let Some(ch) = parse_channel_from_line(t) {
            if (0x11..=0x19).contains(&ch) { has11 = true; }
            if (0x21..=0x29).contains(&ch) { has21 = true; }
        }
    }
    if player == Some(1) { return true; }
    if player.is_none() { return has11 && !has21; }
    false
}

fn is_sp_chart_file(path: &Path) -> Result<bool> {
    let text = decode_text(&fs::read(path)?);
    Ok(is_sp_chart_text(&text))
}

fn collect_map_tasks(target: &Path) -> Result<Vec<MapTask>> {
    if !target.exists() || !target.is_dir() { bail!("Input folder not found: {}", target.display()); }
    let mut out = Vec::<MapTask>::new();
    let charts_root = collect_sp_chart_files_in_dir(target)?;
    if !charts_root.is_empty() {
        out.push(MapTask { dir: target.to_path_buf(), charts: charts_root });
    }
    for ent in fs::read_dir(target)? {
        let p = ent?.path();
        if p.is_dir() {
            let charts = collect_sp_chart_files_in_dir(&p)?;
            if !charts.is_empty() {
                out.push(MapTask { dir: p, charts });
            }
        }
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    Ok(out)
}

fn parse_opts(args: &[OsString]) -> Result<(PathBuf, Opts)> {
    if args.len() < 2 { bail!("Usage: bmX_to_osz_rust.exe <map_folder OR root_folder_with_map_subfolders> [-add7k|-only7k] [-addvideo]"); }
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

fn parse_bms_from_text(path: &Path, text: &str, cut_scratch: bool) -> Result<(BmsMeta, Vec<Note>, Vec<Tp>, Vec<AutoSample>)> {
    let mut m = BmsMeta { bpm: 130.0, rank: -1, ..Default::default() };
    let mut ev = Vec::<Ev>::new();
    let mut token_base: u16 = 36;
    let (mut has16, mut has17, mut has1819) = (false, false, false);

    for raw in text.lines() {
        let t = raw.trim();
        if t.is_empty() || !t.starts_with('#') { continue; }
        let u = t.to_ascii_uppercase();
        let b = t.as_bytes();
        if let Some(v) = u.strip_prefix("#TITLE ") { m.title = t[t.len()-v.len()..].trim().to_string(); continue; }
        if let Some(v) = u.strip_prefix("#ARTIST ") { m.artist = t[t.len()-v.len()..].trim().to_string(); continue; }
        if let Some(v) = u.strip_prefix("#GENRE ") { m.genre = t[t.len()-v.len()..].trim().to_string(); continue; }
        if let Some(v) = u.strip_prefix("#DIFFICULTY ") { m.difficulty = t[t.len()-v.len()..].trim().to_string(); continue; }
        if let Some(v) = u.strip_prefix("#PLAYLEVEL ") { m.playlevel = t[t.len()-v.len()..].trim().to_string(); continue; }
        if let Some(v) = u.strip_prefix("#RANK ") { m.rank = t[t.len()-v.len()..].trim().parse().unwrap_or(-1); continue; }
        if let Some(v) = u.strip_prefix("#BPM ") { m.bpm = t[t.len()-v.len()..].trim().parse().unwrap_or(m.bpm); continue; }
        if let Some(v) = u.strip_prefix("#BASE ") {
            let n = t[t.len()-v.len()..].trim().parse::<u16>().unwrap_or(36);
            token_base = if n == 62 { 62 } else { 36 };
            continue;
        }

        if b.len() >= 7 && b[0] == b'#' && b[1].is_ascii_digit() && b[2].is_ascii_digit() && b[3].is_ascii_digit() && &u[4..6] == "02" && b[6] == b':' {
            let mnum = t[1..4].parse::<i32>().unwrap_or(0);
            if let Ok(v) = t[7..].trim().parse::<f64>() {
                if v > 0.0 {
                    m.measure_len.insert(mnum, v);
                }
            }
            continue;
        }
        if t.len() >= 8 && t[..4].eq_ignore_ascii_case("#WAV") {
            if let Some(id) = tok_bx(&t.as_bytes()[4..6], token_base) {
                let val = t[6..].trim().to_string();
                if !val.is_empty() { m.wav.insert(id, val); }
            }
            continue;
        }
        if t.len() >= 8 && t[..4].eq_ignore_ascii_case("#BPM") && !t.as_bytes()[4].is_ascii_whitespace() {
            if let Some(id) = tok_bx(&t.as_bytes()[4..6], token_base) {
                if let Ok(v) = t[6..].trim().parse::<f64>() { m.bpm_ext.insert(id, v); }
            }
            continue;
        }
        if t.len() >= 9 && t[..5].eq_ignore_ascii_case("#STOP") {
            if let Some(id) = tok_bx(&t.as_bytes()[5..7], token_base) {
                if let Ok(v) = t[7..].trim().parse::<f64>() { m.stop_ext.insert(id, v); }
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

    if m.title.is_empty() { m.title = path.file_stem().and_then(|s| s.to_str()).unwrap_or("Unknown").to_string(); }
    if m.artist.is_empty() { m.artist = "Unknown Artist".to_string(); }

    let mut max_measure = 0i32;
    for e in &ev {
        let mnum = e.pos.floor() as i32;
        if mnum > max_measure { max_measure = mnum; }
    }
    for &k in m.measure_len.keys() {
        if k > max_measure { max_measure = k; }
    }
    let mut prefix = vec![0.0f64; (max_measure.max(0) as usize) + 2];
    for i in 0..=max_measure.max(0) as usize {
        let ml = *m.measure_len.get(&(i as i32)).unwrap_or(&1.0);
        prefix[i + 1] = prefix[i] + 4.0 * ml;
    }
    let mut ev2: Vec<(f64, Ev)> = ev.into_iter().map(|e| {
        let mnum = e.pos.floor() as i32;
        let frac = e.pos - mnum as f64;
        let ml = *m.measure_len.get(&mnum).unwrap_or(&1.0);
        let beats = prefix[mnum.max(0) as usize] + frac * (4.0 * ml);
        (beats, e)
    }).collect();
    if ev2.len() > 1 {
        ev2.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.ch.cmp(&b.1.ch))
        });
    }

    let mut bpm = if m.bpm > 0.0 { m.bpm } else { 130.0 };
    let mut beats_cur = 0.0;
    let mut ms_cur = 0.0;
    let mut tps = vec![Tp { t: 0, bpm }];
    let mut notes = Vec::<Note>::new();
    let mut autos = Vec::<AutoSample>::new();
    let mut ln_open: HashMap<i32, (i32, u16)> = HashMap::new();

    for (beats_abs, e) in ev2 {
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
                if let Some(v) = m.bpm_ext.get(&e.id) {
                    if *v > 0.0 {
                        bpm = *v;
                        tps.push(Tp { t: now, bpm });
                    }
                }
            }
            EvKind::Stop => {
                if let Some(v) = m.stop_ext.get(&e.id) {
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
                    if let Some((start, swav)) = ln_open.remove(&lane) {
                        let mut end = now;
                        if end <= start {
                            end = start + 1;
                        }
                        notes.push(Note { lane, t: start, end, ln: true, wav: swav });
                        if e.id > 0 {
                            autos.push(AutoSample { t: end, wav: e.id });
                        }
                    } else {
                        ln_open.insert(lane, (now, e.id));
                    }
                }
            }
        }
    }

    for (lane, (start, swav)) in ln_open {
        notes.push(Note { lane, t: start, end: start + 120, ln: true, wav: swav });
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
    Ok((m, notes, tps, autos))
}

fn build_osu_bytes(meta: &BmsMeta, notes: &[Note], tp: &[Tp], autos: &[AutoSample], keys: i32, bg_name: &str, video_name: &str, display_version: &str) -> Result<Vec<u8>> {
    let mut f = Vec::<u8>::with_capacity(256 * 1024);
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
    writeln!(f, "Title:{}\r", meta.title)?;
    writeln!(f, "TitleUnicode:{}\r", meta.title)?;
    writeln!(f, "Artist:{}\r", meta.artist)?;
    writeln!(f, "ArtistUnicode:{}\r", meta.artist)?;
    writeln!(f, "Creator:bme_to_osu\r")?;
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
        if let Some(w) = meta.wav.get(&a.wav) {
            let s = Path::new(w).file_name().and_then(|x| x.to_str()).unwrap_or(w);
            writeln!(f, "Sample,{},0,\"{}\",100\r", a.t, s)?;
        }
    }

    writeln!(f, "\r")?;
    writeln!(f, "[TimingPoints]\r")?;
    for t in tp { writeln!(f, "{},{}{},4,2,0,0,1,0\r", t.t, "", fmt_g15(60000.0 / t.bpm))?; }

    writeln!(f, "\r")?;
    writeln!(f, "[HitObjects]\r")?;
    for n in notes {
        let x = ((512.0 / keys as f64) * n.lane as f64 + (256.0 / keys as f64)).floor() as i32;
        let sample = meta.wav.get(&n.wav)
            .map(|v| Path::new(v).file_name().and_then(|x| x.to_str()).unwrap_or(""))
            .unwrap_or("");
        if !n.ln {
            if sample.is_empty() { writeln!(f, "{},192,{},1,0,0:0:0:0:\r", x, n.t)?; }
            else { writeln!(f, "{},192,{},1,0,0:0:0:100:{}\r", x, n.t, sample)?; }
        } else {
            if sample.is_empty() { writeln!(f, "{},192,{},128,0,{}:0:0:0:0:\r", x, n.t, n.end)?; }
            else { writeln!(f, "{},192,{},128,0,{}:0:0:0:100:{}\r", x, n.t, n.end, sample)?; }
        }
    }
    Ok(f)
}

fn osu_has_hitobjects(data: &[u8]) -> bool {
    let s = String::from_utf8_lossy(data);
    let Some(idx) = s.find("[HitObjects]") else { return false; };
    s[idx..].lines().skip(1).any(|l| !l.trim().is_empty())
}

fn collect_bg_image_names(exe_dir: &Path) -> Result<Vec<String>> {
    let bg_dir = exe_dir.join("bg");
    let mut names = Vec::<String>::new();
    if bg_dir.is_dir() {
        for ent in fs::read_dir(&bg_dir)? {
            let p = ent?.path();
            if !p.is_file() { continue; }
            let name = p.file_name().and_then(|x| x.to_str()).unwrap_or("");
            if name.is_empty() { continue; }
            let low = name.to_ascii_lowercase();
            if low.ends_with(".png") || low.ends_with(".jpg") || low.ends_with(".jpeg") || low.ends_with(".bmp") || low.ends_with(".webp") {
                names.push(name.to_string());
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
    let mut pack = Vec::<PackItem>::new();
    let mut seen = HashSet::<String>::new();
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
        let name = p.file_name().and_then(|x| x.to_str()).unwrap_or("").to_string();
        if name.is_empty() { continue; }
        let low_name = name.to_ascii_lowercase();
        if low_name == "1.png" && p.parent() == Some(map_dir) {
            bg = name.clone();
        }
        let ext = p.extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase();
        let is_video = matches!(ext.as_str(), "mp4" | "avi" | "mkv" | "webm" | "mov");
        if include_video && first_video.is_empty() && is_video {
            if let Ok(md) = fs::metadata(&p) {
                if md.len() >= 5 * 1024 * 1024 {
                    first_video = name.clone();
                    first_video_path = p.clone();
                }
            }
        }
        let is_audio = matches!(ext.as_str(), "wav" | "ogg" | "mp3");
        if is_audio && !low_name.eq_ignore_ascii_case("preview_auto_generator.wav") {
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
    let mut central: Vec<(Vec<u8>, u32, u32, u32)> = Vec::new();
    let mut buf = vec![0u8; 1024 * 1024];
    for p in files {
        let name = p.name.as_bytes().to_vec();
        let mut hasher = Crc32Hasher::new();
        let mut sz: u32 = 0;
        let off = z.stream_position()? as u32;
        z.write_all(&0x04034B50u32.to_le_bytes())?;
        z.write_all(&20u16.to_le_bytes())?;
        z.write_all(&0x0008u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u32.to_le_bytes())?;
        z.write_all(&0u32.to_le_bytes())?;
        z.write_all(&0u32.to_le_bytes())?;
        z.write_all(&(name.len() as u16).to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&name)?;
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
        let crc = hasher.finalize();
        z.write_all(&0x08074B50u32.to_le_bytes())?;
        z.write_all(&crc.to_le_bytes())?;
        z.write_all(&sz.to_le_bytes())?;
        z.write_all(&sz.to_le_bytes())?;
        central.push((name, crc, sz, off));
    }
    let cd_off = z.stream_position()? as u32;
    for (name, crc, sz, off) in &central {
        z.write_all(&0x02014B50u32.to_le_bytes())?;
        z.write_all(&20u16.to_le_bytes())?;
        z.write_all(&20u16.to_le_bytes())?;
        z.write_all(&0x0008u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&crc.to_le_bytes())?;
        z.write_all(&sz.to_le_bytes())?;
        z.write_all(&sz.to_le_bytes())?;
        z.write_all(&(name.len() as u16).to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u16.to_le_bytes())?;
        z.write_all(&0u32.to_le_bytes())?;
        z.write_all(&off.to_le_bytes())?;
        z.write_all(name)?;
    }
    let cd_end = z.stream_position()? as u32;
    z.write_all(&0x06054B50u32.to_le_bytes())?;
    z.write_all(&0u16.to_le_bytes())?;
    z.write_all(&0u16.to_le_bytes())?;
    z.write_all(&(central.len() as u16).to_le_bytes())?;
    z.write_all(&(central.len() as u16).to_le_bytes())?;
    z.write_all(&(cd_end - cd_off).to_le_bytes())?;
    z.write_all(&cd_off.to_le_bytes())?;
    z.write_all(&0u16.to_le_bytes())?;
    Ok(())
}

fn process_map(task: &MapTask, out_dir: &Path, exe_dir: &Path, opts: Opts, bg_names: &[String]) -> Result<()> {
    if task.charts.is_empty() { return Ok(()); }
    let map_dir = &task.dir;
    let map_name = map_dir.file_name().and_then(|s| s.to_str()).unwrap_or("map").to_string();
    let (bg_name, video_name, mut pack_items) = collect_assets(map_dir, exe_dir, opts.addvideo, bg_names)?;
    let vid = if opts.addvideo { video_name.clone() } else { String::new() };
    let chart_jobs: Result<Vec<Vec<PackItem>>> = task.charts
        .par_iter()
        .map(|chart| -> Result<Vec<PackItem>> {
            let mut out = Vec::<PackItem>::new();
            let text = decode_text(&fs::read(chart)?);
            if !is_sp_chart_text(&text) {
                return Ok(out);
            }
            let raw_stem = chart.file_stem().and_then(|s| s.to_str()).unwrap_or("chart");
            let stem = trim_ascii_ws(raw_stem);
            if stem.is_empty() {
                return Ok(out);
            }
            if !opts.only7k {
                let (meta, notes8, tp, autos) = parse_bms_from_text(chart, &text, false)?;
                let ver = build_display_version(stem, &meta.playlevel);
                let osu_data = build_osu_bytes(&meta, &notes8, &tp, &autos, 8, &bg_name, &vid, &ver)?;
                if osu_has_hitobjects(&osu_data) {
                    out.push(PackItem { src: None, mem: Some(osu_data), name: format!("{}.osu", stem) });
                }
            }
            if opts.add7k || opts.only7k {
                let (meta, notes7, tp, autos) = parse_bms_from_text(chart, &text, true)?;
                let ver = build_display_version(stem, &meta.playlevel);
                let osu_data = build_osu_bytes(&meta, &notes7, &tp, &autos, 7, &bg_name, &vid, &ver)?;
                if osu_has_hitobjects(&osu_data) {
                    out.push(PackItem { src: None, mem: Some(osu_data), name: format!("{}_7k.osu", stem) });
                }
            }
            Ok(out)
        })
        .collect();

    let mut pack_seen: HashSet<String> = pack_items.iter().map(|p| p.name.to_ascii_lowercase()).collect();
    for set in chart_jobs? {
        for item in set {
            if let Some(data) = item.mem {
                pack_push_mem_unique(&mut pack_items, &mut pack_seen, data, item.name);
            }
        }
    }

    let osz = out_dir.join(format!("{}.osz", map_name));
    if pack_items.len() > 1 {
        pack_items.sort_by(|a, b| a.name.cmp(&b.name));
    }
    build_osz_store(&osz, &pack_items)?;
    println!("Created: {}", osz.display());
    Ok(())
}

fn main() -> Result<()> {
    let args_os: Vec<OsString> = env::args_os().collect();

    let t0 = Instant::now();
    eprintln!("[boot] process started");
    let (target, opts) = parse_opts(&args_os)?;
    eprintln!("[boot] args parsed in {} ms", t0.elapsed().as_millis());
    eprintln!("Starting: {}", target.display());
    let map_tasks = if target.is_dir() && has_chart_ext_in_dir(&target)? {
        let charts = collect_chart_files_in_dir(&target)?;
        if charts.is_empty() { Vec::new() } else { vec![MapTask { dir: target.clone(), charts }] }
    } else {
        collect_map_tasks(&target)?
    };
    if map_tasks.is_empty() { bail!("No SP chart folders with .bme/.bms found in: {}", target.display()); }
    eprintln!("[boot] map scan done in {} ms", t0.elapsed().as_millis());
    eprintln!("MapsFound: {}", map_tasks.len());

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
            eprintln!("Processing: {}", m.dir.display());
            if let Err(e) = process_map(m, &out_dir, &exe_dir, opts, &bg_names) {
                eprintln!("Failed {}: {e}", m.dir.display());
            }
        }
    }
    println!("ExecutionTimeMs: {}", t0.elapsed().as_millis());
    Ok(())
}
