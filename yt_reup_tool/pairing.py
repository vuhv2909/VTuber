from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PairRow:
    index: int
    music_path: str
    video_path: str
    status: str

    @property
    def output_base(self) -> str:
        return f"output_{self.index}"


def shorten_name(path: str) -> str:
    if not path:
        return ""
    return Path(path).name


def build_pair_rows(music_files: list[str], video_files: list[str]) -> list[PairRow]:
    max_len = max(len(music_files), len(video_files))
    rows: list[PairRow] = []
    for index in range(max_len):
        music_path = music_files[index] if index < len(music_files) else ""
        video_path = video_files[index] if index < len(video_files) else ""
        if music_path and video_path:
            status = "Ready"
        elif music_path:
            status = "Missing video"
        else:
            status = "Missing music"
        rows.append(
            PairRow(
                index=index,
                music_path=music_path,
                video_path=video_path,
                status=status,
            )
        )
    return rows
