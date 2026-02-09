#自建数据格式，将数据转换为符合框架输入的数据，用于retriever server的数据库构建，
import json
from typing import Any, Dict, Iterable, List, Optional, Set

class Section:
    def __init__(self, title_text, page_idx,page_size):
        self.title = title_text
        self.page_idx = page_idx
        self.content_blocks = []  # 存储原始 block
        self.combined_text = ""
        self.merged_bbox = None   # [xmin, ymin, xmax, ymax]
        self.page_size = page_size

    def add_block(self, text, bbox):
        if not text: return
        self.content_blocks.append({"text": text, "bbox": bbox})
        self.combined_text += ("\\"+ text )
        self._update_bbox(bbox)

    def _update_bbox(self, bbox):
        if not bbox or len(bbox) != 4: return
        if self.merged_bbox is None:
            self.merged_bbox = list(bbox)
        else:
            # 合并矩形框：取最小的左上角，最大的右下角
            self.merged_bbox[0] = min(self.merged_bbox[0], bbox[0])
            self.merged_bbox[1] = min(self.merged_bbox[1], bbox[1])
            self.merged_bbox[2] = max(self.merged_bbox[2], bbox[2])
            self.merged_bbox[3] = max(self.merged_bbox[3], bbox[3])

    def to_markdown(self):
        # 生成该章节的 Markdown 表达
        return f"## {self.title}{self.combined_text.strip()}\n"

def process_miner_u_json(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    sections = []
    current_section = None

    # JSON 结构外层是 pages
    for page in data.get("pages", []):
        page_num = page.get("page_num")
        page_size = page.get("page_size")
        
        for block in page.get("blocks", []):
            b_type = block.get("type")
            content = block.get("content", "")
            bbox = block.get("bbox", [])

            #如果是公式则处理
            if block.get("type") == "equation" and block.get("format") == "latex":
                latex = content.strip()
                content = f"$$\n{latex}\n$$"
            #如果是图注释或图
            if block.get("type") == "figure_caption" or block.get("type") == "figure":
                content = f"![{content}]" + f"({block.get("img_url","")})\n"

            # 遇到标题，视为新章节的开始
            if b_type == "title":
                # 如果之前有正在处理的章节，存入列表
                if current_section:
                    sections.append(current_section)
                
                current_section = Section(content, page_num,page_size)
                # 标题本身的 bbox 也可以计入合并
                current_section._update_bbox(bbox)
                
            # 如果是文本块或公式块，归入当前章节
            elif b_type in {'text', 'footer', 'table', 'header', 'figure_caption', 'table_caption', 'equation', 'cate', 'blank', 'sider', 'figure'}:
                # if current_section:
                #     current_section.add_block(content, bbox)
                # else:
                #     # 处理还没遇到标题前的正文（如前言）
                #     current_section = Section("Preamble", page_num)
                #     current_section.add_block(content, bbox)
                # ⛔️ 不同页，不允许合并 bbox
                if not current_section:
                    current_section = Section("Preamble", page_num,page_size)
                if page_num != current_section.page_idx:
                    sections.append(current_section)
                    current_section = Section(current_section.title + " (cont.)", page_num,page_size)
                
                current_section.add_block(content, bbox)

    # 最后一个章节
    if current_section:
        sections.append(current_section)
    
    return sections

import os
def _save_jsonl(rows: Iterable[Dict[str, Any]], file_path: str) -> None:

    with open(file_path, "w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
if __name__ == "__main__":
    # json_path = "/mnt/d/wenjian/vscodeworkspace/UltraRag/UltraRAG/corpora/AQ/AQ 1011-2005煤矿在用主通风机系统安全检测检验规范.json"
    # # #按照json去找块：
    # sections = process_miner_u_json(json_path)


    # output_data = []
    # for sec in sections:
    #     output_data.append({
    #         "title": sec.title,
    #         "page": sec.page_idx,
    #         "merged_bbox": sec.merged_bbox,
    #         "markdown": sec.to_markdown()
    #     })

    json_filepath = "/mnt/d/wenjian/vscodeworkspace/UltraRag/UltraRAG/corpora/AQ"
    from pathlib import Path
    from tqdm import tqdm
    root = Path(json_filepath)
    path_list = list(root.rglob("*.json"))

    chunked_documents = []
    current_chunk_id = 0

    for f_json_path in tqdm(path_list, desc=f"pdf-->json file", unit="f"):
        json_name =f_json_path.stem
        sections = process_miner_u_json(f_json_path)

        for sec in sections:
            meta_chunk = {
                "id": current_chunk_id,
                "title": sec.title,
                "contents": f"\\{sec.page_size}\\{sec.page_idx}\\{sec.merged_bbox}\\{json_name}\\{sec.to_markdown()}",
            }
            chunked_documents.append(meta_chunk)
            current_chunk_id += 1
    chunk_md_file_path ="/mnt/d/wenjian/vscodeworkspace/UltraRag/UltraRAG/ui/backend/chunck_all.jsonl"
    _save_jsonl(chunked_documents, chunk_md_file_path)

    """
    最后生成的contents{"\\0\\[122, 53, 333, 127]\\AQ 1011-2005煤矿在用主通风机系统安全检测检验规范\\## Preamble\\}
    格式，页码\\bbox\\文件名称\\标题\\标题下的内容
    """


    # with open("processed_result.json", "w", encoding="utf-8") as f:
    #     json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    # print("\n✅ 处理完成！请打开 'processed_result.json' 查看详细数据。")

    # with open("processed_result.json", "r", encoding="utf-8") as f:
    #     data = json.load(f)
    # with open('result_mk.md',"w",encoding="utf-8") as f:
    #     for d in data:
    #         f.write(d['markdown'])

    # import json

    # with open(json_path, "r", encoding="utf-8") as f:
    #     data = json.load(f)

    # types = set()

    # for page in data.get("pages", []):
    #     for block in page.get("blocks", []):
    #         t = block.get("type")
    #         if t:
    #             types.add(t)

    # print(types)
