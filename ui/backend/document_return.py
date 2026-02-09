import re
import os
import numpy as np
import jieba


def jaccard_similarity(a, b): #集合重合度
    set_a = set(jieba.cut(a))
    set_b = set(jieba.cut(b))
    res = len(set_a & set_b) / len(set_a | set_b) if len(set_a | set_b) > 0 else 0
    return res


# 扫描block，找出候选
def find_candidate_blocks(md_text, pages, threshold=0.30):
    hits = []

    for page in pages:
        pno = page["page_num"]
        for blk in page["blocks"]:
            content = blk.get("content", "")
            score = jaccard_similarity(md_text, content)
            # print(score,blk["idx"])
            

            if score >= threshold:
                hits.append({
                    "page": pno,
                    "idx": blk["idx"],
                    "bbox": blk["bbox"],
                    "score": score
                })
    # print(hits)
    return hits

# 聚合相邻block（同页 + 连续）
def group_adjacent_blocks(blocks, idx_gap=1, y_gap=20):
    blocks = sorted(blocks, key=lambda x: (x["page"], x["idx"]))
    groups = []
    cur = []

    for blk in blocks:
        if not cur:
            cur = [blk]
            continue

        prev = cur[-1]
        same_page = blk["page"] == prev["page"]
        idx_close = blk["idx"] - prev["idx"] <= idx_gap
        y_close = abs(blk["bbox"][1] - prev["bbox"][3]) <= y_gap

        if same_page and (idx_close or y_close):
            cur.append(blk)
        else:
            groups.append(cur)
            cur = [blk]

    if cur:
        groups.append(cur)

    return groups

# bbox合并
def merge_bboxes(blocks):
    return [
        min(b["bbox"][0] for b in blocks),
        min(b["bbox"][1] for b in blocks),
        max(b["bbox"][2] for b in blocks),
        max(b["bbox"][3] for b in blocks),
    ]

##主函数入口（接口使用）
def locate_md_in_pdf(md_hit_text, pages):
    candidates = find_candidate_blocks(md_hit_text, pages)
    # print(candidates)
    if not candidates:
        return None

    groups = group_adjacent_blocks(candidates)
    results = []

    for g in groups:
        bbox = merge_bboxes(g)
        conf = sum(b["score"] for b in g) / len(g)
        results.append({
            "page": g[0]["page"],
            "bbox": bbox,
            "confidence": round(conf, 3)
        })

    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results[0]   # 取最可信的
#bbox，页面左上角原点，左上角与右下角坐标

if __name__ == '__main__':
    md_hit_text = "在距风压测点 20 m 内的巷道中，用气压计测量绝对静压，用干、湿温度计测量干、湿温度。每调节工况 1 次测量 3 次，按式(1)计算空气密度取其算术平均值：\
        \\rho=3.484\\times 10^{-3}\\,\\frac{p_{0}-0.377\\,9\\Phi p,sat}{273+t} \\qquad\\ldots\\ldots\\ldots\\ldots\\ldots\\ldots\\ldots(1) 式中：$\\rho$——空气密度,$kg/m^{3}$; \
        $p _ {0}$——大气压力，Pa；$\\Phi$——空气的相对湿度，%；$p _ {sat}$ ——温度为 $t^{\\circ}C$ 时空气的绝对饱和水蒸气压力，Pa；t——空气的温度,$℃$。"

    import json
    json_path = "/mnt/d/wenjian/vscodeworkspace/UltraRag/UltraRAG/ui/backend/processed_result.json"
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # pages = data['pages']
    # # print(len(pages))

    # result = locate_md_in_pdf(md_hit_text, pages)
    # print(result)  #{'page': 5, 'bbox': [129, 1286, 1106, 1403], 'confidence': 0.41}
    # print(type(result["page"]))

    

