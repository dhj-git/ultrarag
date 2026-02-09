import asyncio
import ast
import json
import os
import re
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from fastmcp.exceptions import ToolError

from tqdm import tqdm
from ultrarag.server import UltraRAG_MCP_Server

app = UltraRAG_MCP_Server("corpus")

def _save_jsonl(rows: Iterable[Dict[str, Any]], file_path: str) -> None:
    """Save rows to a JSONL file.

    Args:
        rows: Iterable of dictionaries to save
        file_path: Path to the output JSONL file
    """
    out_dir = Path(file_path).parent
    if out_dir and str(out_dir) != ".":
        os.makedirs(out_dir, exist_ok=True)

    with open(file_path, "w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def reflow_paragraphs(text: str) -> str:
    """Intelligently remove hard line breaks within paragraphs and merge incorrectly split paragraphs.

    The function:
    1) Splits by blank lines first; within a paragraph, if the previous line doesn't end with
       sentence-ending punctuation, merge it with the next line.
    2) If a paragraph doesn't end with sentence-ending punctuation and the next paragraph
       appears to be a continuation, merge across blank lines.
    3) Handles trailing hyphen word breaks.

    Args:
        text: Input text to reflow

    Returns:
        Reflowed text string
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    end_punct_re = re.compile(r"[。！？!?；;…]\s*[”’」』》）】]*\s*$")
    next_start_re = re.compile(r'^[\u4e00-\u9fff0-9a-zA-Z“"‘’《（(【\[「『<]')

    def merge_lines_within_paragraph(para: str) -> str:
        lines = para.split("\n")
        segs: List[str] = []
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            if not segs:
                segs.append(ln)
                continue
            prev = segs[-1]
            should_join = not end_punct_re.search(prev)
            if should_join:
                if prev.endswith("-") and len(prev) > 1:
                    segs[-1] = prev[:-1] + ln
                else:
                    segs[-1] = prev + " " + ln
            else:
                segs.append(ln)
        joined = " ".join(segs)
        return re.sub(r"\s{2,}", " ", joined).strip()

    # First pass: merge lines within paragraphs
    raw_paras = re.split(r"\n{2,}", text)
    paras = [merge_lines_within_paragraph(p) for p in raw_paras if p.strip()]

    # Second pass: merge across paragraphs (handle incorrect blank lines causing sentence breaks)
    merged: List[str] = []
    for p in paras:
        if not merged:
            merged.append(p)
            continue
        prev = merged[-1]
        if prev and (not end_punct_re.search(prev)) and next_start_re.match(p):
            connector = "" if prev.endswith("-") else " "
            merged[-1] = re.sub(
                r"\s{2,}", " ", (prev.rstrip("-") + connector + p).strip()
            )
        else:
            merged.append(p)

    return "\n\n".join(merged).strip()

def clean_text(text: str) -> str:
    """Clean text by normalizing whitespace and line breaks.

    Args:
        text: Input text to clean

    Returns:
        Cleaned text string
    """
    if not text:
        return ""
    text = text.replace("\u3000", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

# myserver:
#   raw_md_path: corpora/AQ
#   chunk_md_path: corpora/chunks_md_result.jsonl
import chardet
def read_markdown_file(path: str) -> str:
    p = Path(path)
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raw = p.read_bytes()
        encoding = chardet.detect(raw)["encoding"]
        return raw.decode(encoding)

@app.tool(
    output="raw_md_path,chunk_md_path->None"
)
async def chunk_md_documents(
    raw_md_path: str,
    chunk_md_path: Optional[str] = None,

) -> None:
    """Chunk documents using various chunking strategies.

    Args:
        raw_md_path: Path to JSONL file containing documents to chunk
        
        chunk_md_path: Optional output path for chunked documents (defaults to project output directory)

    """
    try:
        from langchain_text_splitters import MarkdownHeaderTextSplitter
    except ImportError:
        err_msg = (
            "MarkdownHeaderTextSplitter not installed. Please `pip install langchain-text-splitters`."
        )
        app.logger.error(err_msg)
        raise ToolError(err_msg)

    if chunk_md_path is None:
        current_file = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file))
        output_dir = os.path.join(project_root, "output", "corpus")
        chunk_md_path = os.path.join(output_dir, "chunks_md_result.jsonl")
    else:
        chunk_md_path = str(chunk_md_path)
        output_dir = os.path.dirname(chunk_md_path)
    os.makedirs(output_dir, exist_ok=True)


    md_text = read_markdown_file(raw_md_path)
    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[("##", "section")]
    )
    docs = header_splitter.split_text(md_text)

    chunked_documents = []
    current_chunk_id = 0
    for doc in tqdm(docs, desc=f"Chunking (MarkdownHeaderTextSplitter)", unit="doc"):
        meta_chunk = {
            "id": current_chunk_id,
            "title": doc.metadata.get("section", ""),
            "contents": reflow_paragraphs(clean_text(doc.page_content)),
        }
        chunked_documents.append(meta_chunk)
        current_chunk_id += 1

    _save_jsonl(chunked_documents, chunk_md_path)

from pathlib import Path

def find_md_files(root_dir: str):
    root = Path(root_dir)
    return list(root.rglob("*.md"))

@app.tool(
    output="raw_md_file_path,chunk_md_file_path->None"
)
async def chunk_md_file_documents(
    raw_md_file_path: str,
    chunk_md_file_path: Optional[str] = None,

) -> None:
    """Chunk documents using various chunking strategies.

    Args:
        raw_md_file_path: Path to JSONL file containing documents to chunk
        
        chunk_md_file_path: Optional output path for chunked documents (defaults to project output directory)

    """
    try:
        from langchain_text_splitters import MarkdownHeaderTextSplitter
    except ImportError:
        err_msg = (
            "langchain_text_splitters not installed. Please `pip install langchain_text_splitters`."
        )
        app.logger.error(err_msg)
        raise ToolError(err_msg)

    if chunk_md_file_path is None:
        current_file = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file))
        output_dir = os.path.join(project_root, "output", "corpus")
        chunk_md_file_path = os.path.join(output_dir, "chunks_all_md_result.jsonl")
    else:
        chunk_md_file_path = str(chunk_md_file_path)
        output_dir = os.path.dirname(chunk_md_file_path)
    os.makedirs(output_dir, exist_ok=True)

    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[("##", "section")]
    )

    chunked_documents = []
    current_chunk_id = 0
    md_files = find_md_files(raw_md_file_path)
    for f in tqdm(md_files, desc=f"Chunking (MarkdownHeaderTextSplitter)", unit="f"):
        doc_name =f.stem
        f = str(f)
        md_text = read_markdown_file(f)
   
        docs = header_splitter.split_text(md_text)

    
        for doc in docs:
            meta_chunk = {
                "id": current_chunk_id,
                "title": doc.metadata.get("section", ""),
                "contents": "\\"+doc_name+"\\"+doc.metadata.get("section", "")+"\\"+reflow_paragraphs(clean_text(doc.page_content)),
            }
            chunked_documents.append(meta_chunk)
            current_chunk_id += 1

    _save_jsonl(chunked_documents, chunk_md_file_path)

pattern = re.compile(r"\\([^\\]+)\\")
def extract_filename(text: str):
    m = pattern.search(text)
    return m.group(1) if m else None

## 获取检索到的原文档
@app.tool(output="ret_psg->path_list")
def get_citation_file(
    ret_citation_psg: List[List[str]],
) -> Dict[str, Any]:
    """获取引用文档的地址.
    Args:
        ret_psg：文档字符串列表的列表
    Returns:
        检索到文档的文档路径列表
    """
    result_psg = []
    current_file = os.path.abspath(__file__)
    middel_file = os.path.dirname(os.path.dirname(current_file))
    project_root = os.path.dirname(os.path.dirname(middel_file))
    # /mnt/d/wenjian/vscodeworkspace/UltraRag/UltraRAG
    data_dir = os.path.join(project_root,"corpus","AQ_pdf_json")
    for docs_list in ret_citation_psg:
        # print(docs_list)
        for text in docs_list:
            file_name = extract_filename(text)
            file_path = os.path.join(data_dir,f"{file_name}.pdf")
            result_psg.append(file_path)

    return {
        "path_list": result_psg,
    }

if __name__ == "__main__":
    app.run(transport="stdio")

