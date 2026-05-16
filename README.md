primarily for - https://github.com/Glebsin/iidx2bms/

flags:

-add7k
Create both versions: normal 8K and additional 7K (scratch-cut).

-only7k
Create only the 7K version (scratch-cut), without 8K.

-addvideo
Add a video file from the map folder (if a suitable file longer than 1 minute is found).

Important:
-add7k and -only7k cannot be used together.

usage example: 

bmX_to_osz.exe "C:\Games\LR2oraja Endless Dream\music\maps"

bmX_to_osz.exe "C:\Games\LR2oraja Endless Dream\music\bms\somemap"

bmX_to_osz.exe "C:\Games\LR2oraja Endless Dream\music\othermaps" -add7k -addvideo

bmX_to_osz.exe "C:\Games\LR2oraja Endless Dream\music\bms\somemap" -only7k


how compile: cargo build --release
