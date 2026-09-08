import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Polygon, PathPatch
import matplotlib.path as mpath
import numpy as np

os.makedirs("project_diagrams", exist_ok=True)

# Standard IEEE font configuration
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['mathtext.fontset'] = 'cm'

# ==============================================================================
# Helper Drawing Functions for IEEE Standard Flowchart & Block Architecture
# ==============================================================================

def draw_ieee_terminator(ax, cx, cy, w, h, text, subtext="", fill="#f1f5f9", stroke="#0f172a"):
    """Standard IEEE Flowchart Terminator (Oval/Capsule)"""
    box = FancyBboxPatch((cx - w/2, cy - h/2), w, h,
                         boxstyle=f"round,pad=0.02,rounding_size={h/2}",
                         facecolor=fill, edgecolor=stroke, linewidth=1.5, zorder=3)
    ax.add_patch(box)
    if subtext:
        ax.text(cx, cy + 0.12, text, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)
        ax.text(cx, cy - 0.14, subtext, fontsize=7.5, color='#475569', ha='center', va='center', zorder=4)
    else:
        ax.text(cx, cy, text, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)

def draw_ieee_process(ax, cx, cy, w, h, title, subtitle="", items=[], fill="#ffffff", stroke="#0f172a", line_width=1.5, num=None):
    """Standard IEEE Process Box (Rectangle with optional step number)"""
    # Clean shadow/depth
    rect_shadow = patches.Rectangle((cx - w/2 + 0.04, cy - h/2 - 0.04), w, h,
                                    facecolor='#e2e8f0', edgecolor='none', zorder=2)
    ax.add_patch(rect_shadow)
    
    # Process Rectangle
    rect = patches.Rectangle((cx - w/2, cy - h/2), w, h,
                             facecolor=fill, edgecolor=stroke, linewidth=line_width, zorder=3)
    ax.add_patch(rect)
    
    # Optional Number Badge
    if num:
        badge = patches.Rectangle((cx - w/2, cy + h/2 - 0.32), 0.45, 0.32,
                                  facecolor=stroke, edgecolor='none', zorder=4)
        ax.add_patch(badge)
        ax.text(cx - w/2 + 0.225, cy + h/2 - 0.16, str(num), fontsize=8, fontweight='bold',
                color='#ffffff', ha='center', va='center', zorder=5)

    # Title & Subtitle
    top_y = cy + h/2 - 0.22
    ax.text(cx, top_y, title, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)
    
    if subtitle:
        top_y -= 0.25
        ax.text(cx, top_y, subtitle, fontsize=7.5, fontstyle='italic', color='#475569', ha='center', va='center', zorder=4)
        
    if items:
        item_y = top_y - 0.25
        for itm in items:
            ax.text(cx - w/2 + 0.25, item_y, f"• {itm}", fontsize=7.5, color='#1e293b', va='center', zorder=4)
            item_y -= 0.22

def draw_ieee_io(ax, cx, cy, w, h, title, subtitle="", fill="#f8fafc", stroke="#0f172a", skew=0.3):
    """Standard IEEE Data I/O (Parallelogram)"""
    pts = [
        [cx - w/2 + skew, cy + h/2],
        [cx + w/2, cy + h/2],
        [cx + w/2 - skew, cy - h/2],
        [cx - w/2, cy - h/2]
    ]
    poly_s = Polygon([[p[0]+0.04, p[1]-0.04] for p in pts], closed=True, facecolor='#e2e8f0', edgecolor='none', zorder=2)
    ax.add_patch(poly_s)
    poly = Polygon(pts, closed=True, facecolor=fill, edgecolor=stroke, linewidth=1.5, zorder=3)
    ax.add_patch(poly)
    
    if subtitle:
        ax.text(cx, cy + 0.14, title, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)
        ax.text(cx, cy - 0.14, subtitle, fontsize=7.5, fontstyle='italic', color='#475569', ha='center', va='center', zorder=4)
    else:
        ax.text(cx, cy, title, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)

def draw_ieee_decision(ax, cx, cy, w, h, title, subtitle="", fill="#fdf2f8", stroke="#be185d"):
    """Standard IEEE Decision Block (Diamond)"""
    pts = [
        [cx, cy + h/2],
        [cx + w/2, cy],
        [cx, cy - h/2],
        [cx - w/2, cy]
    ]
    poly_s = Polygon([[p[0]+0.04, p[1]-0.04] for p in pts], closed=True, facecolor='#e2e8f0', edgecolor='none', zorder=2)
    ax.add_patch(poly_s)
    poly = Polygon(pts, closed=True, facecolor=fill, edgecolor=stroke, linewidth=1.5, zorder=3)
    ax.add_patch(poly)
    
    if subtitle:
        ax.text(cx, cy + 0.14, title, fontsize=9, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)
        ax.text(cx, cy - 0.16, subtitle, fontsize=7.5, color='#475569', ha='center', va='center', zorder=4)
    else:
        ax.text(cx, cy, title, fontsize=9, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=4)

def draw_ieee_cylinder(ax, cx, cy, w, h, title, subtitle="", items=[], fill="#f0fdf4", stroke="#15803d"):
    """Standard IEEE Database Cylinder Shape"""
    cap_h = h * 0.18
    # Main cylinder body
    rect = patches.Rectangle((cx - w/2, cy - h/2 + cap_h/2), w, h - cap_h,
                             facecolor=fill, edgecolor=stroke, linewidth=1.5, zorder=3)
    ax.add_patch(rect)
    # Bottom curve
    bottom_cap = patches.Arc((cx, cy - h/2 + cap_h/2), w, cap_h, angle=0, theta1=180, theta2=360,
                             edgecolor=stroke, linewidth=1.5, zorder=4)
    ax.add_patch(bottom_cap)
    # Top ellipse
    top_cap = patches.Ellipse((cx, cy + h/2 - cap_h/2), w, cap_h,
                              facecolor=fill, edgecolor=stroke, linewidth=1.5, zorder=4)
    ax.add_patch(top_cap)
    
    top_y = cy + h/2 - cap_h - 0.15
    ax.text(cx, top_y, title, fontsize=9, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=5)
    if subtitle:
        top_y -= 0.22
        ax.text(cx, top_y, subtitle, fontsize=7.5, fontstyle='italic', color='#475569', ha='center', va='center', zorder=5)
    if items:
        item_y = top_y - 0.22
        for itm in items:
            ax.text(cx - w/2 + 0.2, item_y, f"• {itm}", fontsize=7, color='#1e293b', va='center', zorder=5)
            item_y -= 0.2

def draw_arrow(ax, start, end, label="", label_pos="right", col="#0f172a", style="-|>"):
    """Clean IEEE Orthogonal/Straight Connector Arrow"""
    arrow = FancyArrowPatch(start, end, arrowstyle="Simple,tail_width=1.8,head_width=6,head_length=6",
                            color=col, linewidth=1, zorder=5)
    ax.add_patch(arrow)
    if label:
        mx = (start[0] + end[0]) / 2 + (0.85 if label_pos == "right" else (-0.85 if label_pos == "left" else 0))
        my = (start[1] + end[1]) / 2 + (0.18 if label_pos == "top" else (-0.18 if label_pos == "bottom" else 0))
        ax.text(mx, my, label, fontsize=7.5, fontweight='bold', color=col, ha='center', va='center',
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#ffffff", edgecolor=col, linewidth=0.7, zorder=6))


# ==============================================================================
# Diagram 1: IEEE Standard Vertical System Flowchart (Fig. 1)
# ==============================================================================
def generate_ieee_architecture_flowchart():
    fig, ax = plt.subplots(figsize=(8.5, 13.5), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 8.5)
    ax.set_ylim(0, 13.5)
    ax.axis('off')

    # Title & IEEE Header
    ax.text(4.25, 13.1, "Real-Time Distributed Twitter Sentiment Analysis",
            fontsize=13, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.25, 12.8, "IEEE Technical Architecture & Sequential Processing Flowchart",
            fontsize=9, fontstyle='italic', color='#475569', ha='center', va='center')

    # 1. Start Terminator
    draw_ieee_terminator(ax, 4.25, 12.2, 2.6, 0.5, "START", "Streaming Pipeline Initialized", "#f1f5f9", "#334155")
    draw_arrow(ax, (4.25, 11.95), (4.25, 11.45), "")

    # 2. Ingestion I/O (Parallelogram)
    draw_ieee_io(ax, 4.25, 10.95, 4.8, 1.0, "Twitter Data Ingestion Layer", "kafka_producer.py (Live Scraper & CSV Stream)", "#f0f9ff", "#0284c7")
    draw_arrow(ax, (4.25, 10.45), (4.25, 9.95), "JSON Records", col="#0284c7")

    # 3. Message Broker (Process)
    draw_ieee_process(ax, 4.25, 9.35, 5.0, 1.2, "Apache Kafka Message Broker", "kafka_producer.py -> localhost:9092",
                      ["Topic: 'numtest' Distributed Partition Buffer", "Real-Time Message Pub/Sub Channel"],
                      fill="#f8fafc", stroke="#475569", num="1")
    draw_arrow(ax, (4.25, 8.75), (4.25, 8.25), "Micro-Batches", col="#475569")

    # 4. Stream Processing (Process)
    draw_ieee_process(ax, 4.25, 7.6, 5.0, 1.3, "PySpark Structured Streaming Engine", "spark_consumer.py (Micro-Batch Processor)",
                      ["Stream Ingestion from Kafka Broker", "Text Sanitization, Regex Tokenizer, StopWords Filter"],
                      fill="#fff7ed", stroke="#ea580c", num="2")
    draw_arrow(ax, (4.25, 6.95), (4.25, 6.45), "Feature Vectors", col="#ea580c")

    # 5. MLlib Model Inference & Decision (Diamond)
    draw_ieee_decision(ax, 4.25, 5.75, 4.6, 1.4, "PySpark MLlib Classifier", "Decision: Logistic Regression (4 Classes)", "#fdf2f8", "#db2777")
    
    # Branches from Decision Diamond
    # Arrow down-left to MongoDB Hot Store
    draw_arrow(ax, (3.2, 5.35), (2.3, 4.5), "Operational", col="#15803d")
    # Arrow down-right to Parquet Cold Store
    draw_arrow(ax, (5.3, 5.35), (6.2, 4.5), "Analytical", col="#0891b2")

    # 6. Dual Storage Tiering (Cylinders)
    draw_ieee_cylinder(ax, 2.3, 3.65, 3.4, 1.5, "Hot Tier: MongoDB", "Operational Sink (NoSQL)",
                       ["Uncompressed JSON Docs", "Sub-second UI Query Latency"],
                       fill="#f0fdf4", stroke="#15803d")
    
    draw_ieee_cylinder(ax, 6.2, 3.65, 3.4, 1.5, "Cold Tier: Parquet", "Data Lakehouse Sink",
                       ["Snappy Columnar Format", "~82% Compression Reduction"],
                       fill="#f0f9ff", stroke="#0891b2")

    # 7. Serving Layer (Flask Dashboard)
    draw_ieee_process(ax, 2.3, 1.75, 3.4, 1.2, "Flask Web Dashboard", "webapp/app.py (Port 8000)",
                      ["Real-Time Sentiment Metrics & Charts", "User Feedback Verification Portal"],
                      fill="#fefce8", stroke="#ca8a04", num="3")
    draw_arrow(ax, (2.3, 2.9), (2.3, 2.35), "Live Data", col="#15803d")

    # 8. Active Learning Loop Box
    draw_ieee_process(ax, 6.2, 1.75, 3.4, 1.2, "Active Learning Retrainer", "spark_retrainer.py",
                      ["Syncs Ground-Truth MongoDB Feedback", "Retrains PySpark PipelineModel"],
                      fill="#fdf2f8", stroke="#db2777", num="4")
    
    # Orthogonal User Feedback Arrow (UI -> Retrainer)
    ax.add_patch(FancyArrowPatch((4.0, 1.75), (4.5, 1.75), arrowstyle="Simple,tail_width=1.5,head_width=5,head_length=5", color="#db2777", zorder=5))
    ax.text(4.25, 1.95, "Feedback", fontsize=7, fontweight='bold', color='#db2777', ha='center', va='center')

    # Feedback Loop arrow (Retrainer up to MLlib Pipeline)
    ax.add_patch(FancyArrowPatch((7.9, 1.75), (8.2, 1.75), arrowstyle="-", color="#db2777", linewidth=1.5, zorder=5))
    ax.add_patch(FancyArrowPatch((8.2, 1.75), (8.2, 7.6), arrowstyle="-", color="#db2777", linewidth=1.5, zorder=5))
    ax.add_patch(FancyArrowPatch((8.2, 7.6), (6.75, 7.6), arrowstyle="Simple,tail_width=1.5,head_width=5,head_length=5", color="#db2777", zorder=5))
    ax.text(8.2, 4.6, "Reload Retrained Model Artifact", fontsize=7, fontweight='bold', color='#db2777',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="square,pad=0.2", facecolor="#ffffff", edgecolor="#db2777", linewidth=0.7))

    # Bottom Caption in IEEE Standard Format
    ax.text(4.25, 0.4, "Fig. 1. Standard IEEE Architecture Flowchart of the Distributed Twitter Sentiment Analysis\nPipeline with Hot/Cold Storage Tiering and Active Learning Retraining Loop.",
            fontsize=8.5, fontweight='bold', color='#0f172a', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/architecture_diagram.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.savefig("project_diagrams/system_flowchart.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated IEEE architecture_diagram.png (Fig. 1)")


# ==============================================================================
# Diagram 2: IEEE PySpark MLlib NLP Pipeline (Fig. 2)
# ==============================================================================
def generate_ieee_nlp_pipeline():
    fig, ax = plt.subplots(figsize=(7.5, 11), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 7.5)
    ax.set_ylim(0, 11)
    ax.axis('off')

    # Title & IEEE Header
    ax.text(3.75, 10.6, "PySpark MLlib Natural Language Processing Pipeline",
            fontsize=12, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(3.75, 10.3, "Sequential Feature Extraction, Term Frequency & Multiclass Inference",
            fontsize=8.5, fontstyle='italic', color='#475569', ha='center', va='center')

    stages = [
        ("Input Stream: Raw Tweet Text", "JSON Payload containing text, author, timestamp", "#f0f9ff", "#0284c7", "Data I/O", 9.5),
        ("Stage 1: Regex Text Sanitizer", "Strips URLs (http/https), @mentions, & special symbols", "#fefce8", "#d97706", "Process", 8.2),
        ("Stage 2: RegexTokenizer", "Splits cleaned text into normalized lowercase tokens", "#f0fdf4", "#059669", "Process", 6.9),
        ("Stage 3: StopWordsRemover", "Eliminates high-frequency stopwords ('the', 'is', 'at')", "#eef2ff", "#4f46e5", "Process", 5.6),
        ("Stage 4: HashingTF + IDF", "Maps tokens into 10,000-dimensional TF-IDF vectors", "#fdf2f8", "#db2777", "Process", 4.3),
        ("Stage 5: LogisticRegressionModel", "Calculates class probabilities across 4 categories", "#f5f3ff", "#7c3aed", "Classifier", 3.0),
        ("Output: Sentiment Classification", "Predictions: Positive (1), Negative (0), Neutral (2), Irrelevant (3)", "#f0fdf4", "#16a34a", "Result", 1.7)
    ]

    for title, desc, bg, stroke, tag, y in stages:
        if "I/O" in tag:
            draw_ieee_io(ax, 3.75, y, 5.4, 0.9, title, desc, fill=bg, stroke=stroke, skew=0.25)
        elif "Result" in tag:
            draw_ieee_terminator(ax, 3.75, y, 5.4, 0.9, title, desc, fill=bg, stroke=stroke)
        else:
            draw_ieee_process(ax, 3.75, y, 5.4, 0.9, title, desc, fill=bg, stroke=stroke)

    for i in range(len(stages) - 1):
        y_start = stages[i][5] - 0.45
        y_end = stages[i+1][5] + 0.45
        draw_arrow(ax, (3.75, y_start), (3.75, y_end), col="#475569")

    # Bottom Caption in IEEE Standard Format
    ax.text(3.75, 0.5, "Fig. 2. PySpark MLlib NLP Pipeline Architecture for Real-Time Text Sanitization,\nTF-IDF Vectorization, and Multiclass Logistic Regression Sentiment Scoring.",
            fontsize=8.5, fontweight='bold', color='#0f172a', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/spark_nlp_pipeline.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated IEEE spark_nlp_pipeline.png (Fig. 2)")


# ==============================================================================
# Diagram 3: IEEE Storage Tiering Architecture (Fig. 3)
# ==============================================================================
def generate_ieee_storage_tiering():
    fig, ax = plt.subplots(figsize=(8.5, 9.5), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 8.5)
    ax.set_ylim(0, 9.5)
    ax.axis('off')

    ax.text(4.25, 9.1, "Distributed Storage Architecture: Hot Tier vs. Cold Lakehouse",
            fontsize=12, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.25, 8.8, "Sub-Second Operational Query Latency vs. Compressed Columnar Lakehouse",
            fontsize=8.5, fontstyle='italic', color='#475569', ha='center', va='center')

    # Stream Producer
    draw_ieee_process(ax, 4.25, 7.8, 5.6, 1.1, "PySpark Structured Streaming Sink Manager", "spark_consumer.py & data_lakehouse.py",
                      ["Dual-write pipeline: Concurrent NoSQL Document & Columnar Append"],
                      fill="#fff7ed", stroke="#ea580c")

    draw_arrow(ax, (2.8, 7.25), (2.8, 6.35), "Hot Path (JSON)", col="#15803d", label_pos="left")
    draw_arrow(ax, (5.7, 7.25), (5.7, 6.35), "Cold Path (Snappy)", col="#0891b2", label_pos="right")

    # Hot Tier
    draw_ieee_cylinder(ax, 2.8, 5.2, 3.4, 1.8, "Hot Store: MongoDB NoSQL", "Operational Document Store",
                       ["Format: JSON Documents", "Write Throughput: High", "Query Latency: < 50ms", "Used for: Real-time UI Ledger"],
                       fill="#f0fdf4", stroke="#15803d")

    # Cold Tier
    draw_ieee_cylinder(ax, 5.7, 5.2, 3.4, 1.8, "Cold Store: Parquet Lakehouse", "Snappy Compressed Columnar Storage",
                       ["Format: Apache Parquet", "Storage Saved: ~82%", "Query Type: OLAP Scan", "Used for: Spark Retraining"],
                       fill="#f0f9ff", stroke="#0891b2")

    draw_arrow(ax, (2.8, 4.3), (2.8, 3.4), "Live Streaming APIs", col="#15803d", label_pos="left")
    draw_arrow(ax, (5.7, 4.3), (5.7, 3.4), "Batch OLAP Reads", col="#0891b2", label_pos="right")

    # Sinks
    draw_ieee_process(ax, 2.8, 2.7, 3.4, 1.1, "Serving Dashboard", "Flask Web UI (Port 8000)",
                      ["Real-time Chart.js KPI updates", "Live teleprinter data stream"],
                      fill="#fefce8", stroke="#ca8a04")

    draw_ieee_process(ax, 5.7, 2.7, 3.4, 1.1, "Batch Retraining Engine", "PySpark MLlib (spark_retrainer.py)",
                      ["Historical trend analytics", "Incremental model updates"],
                      fill="#fdf2f8", stroke="#db2777")

    # Bottom Caption in IEEE Standard Format
    ax.text(4.25, 0.6, "Fig. 3. Dual Storage Tiering Architecture showing separation of Hot Operational MongoDB\nand Cold Analytical Apache Parquet Lakehouse with compression and latency trade-offs.",
            fontsize=8.5, fontweight='bold', color='#0f172a', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/lakehouse_tiering.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated IEEE lakehouse_tiering.png (Fig. 3)")


# ==============================================================================
# Diagram 4: IEEE Active Learning & Retraining Loop (Fig. 4)
# ==============================================================================
def generate_ieee_active_learning():
    fig, ax = plt.subplots(figsize=(7.5, 11), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 7.5)
    ax.set_ylim(0, 11)
    ax.axis('off')

    ax.text(3.75, 10.6, "Human-in-the-Loop Active Learning Pipeline",
            fontsize=12, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(3.75, 10.3, "Continuous Model Adaptation through Interactive Ground-Truth Feedback",
            fontsize=8.5, fontstyle='italic', color='#475569', ha='center', va='center')

    nodes = [
        ("Step 1: Real-Time Stream Inspection", "Human operator reviews live model predictions on dashboard", "#f0f9ff", "#0284c7", 9.4),
        ("Step 2: Interactive Label Verification", "Operator clicks correction badge (+Pos, -Neg, Neu, Irr)", "#fdf2f8", "#db2777", 7.9),
        ("Step 3: Ground-Truth MongoDB Sync", "Document updated with 'is_verified: true' and verified label", "#f0fdf4", "#16a34a", 6.4),
        ("Step 4: PySpark Batch Retraining", "Merges verified feedback with training set and refits pipeline", "#fff7ed", "#ea580c", 4.9),
        ("Step 5: Dynamic Artifact Hot-Reload", "Updated PipelineModel deployed to streaming inference without restart", "#f5f3ff", "#7c3aed", 3.4)
    ]

    for title, desc, bg, stroke, y in nodes:
        draw_ieee_process(ax, 3.75, y, 5.4, 1.0, title, desc, fill=bg, stroke=stroke)

    for i in range(len(nodes) - 1):
        y_start = nodes[i][4] - 0.5
        y_end = nodes[i+1][4] + 0.5
        draw_arrow(ax, (3.75, y_start), (3.75, y_end), col="#475569")

    # Feedback loop line back to top
    ax.add_patch(FancyArrowPatch((6.45, 3.4), (6.9, 3.4), arrowstyle="-", color="#7c3aed", linewidth=1.5, zorder=5))
    ax.add_patch(FancyArrowPatch((6.9, 3.4), (6.9, 9.4), arrowstyle="-", color="#7c3aed", linewidth=1.5, zorder=5))
    ax.add_patch(FancyArrowPatch((6.9, 9.4), (6.45, 9.4), arrowstyle="Simple,tail_width=1.5,head_width=5,head_length=5", color="#7c3aed", zorder=5))
    ax.text(6.9, 6.4, "Continuous Adaptation Loop", fontsize=7.5, fontweight='bold', color='#7c3aed',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="square,pad=0.2", facecolor="#ffffff", edgecolor="#7c3aed", linewidth=0.7))

    # Bottom Caption in IEEE Standard Format
    ax.text(3.75, 1.2, "Fig. 4. Closed-Loop Active Learning Architecture demonstrating real-time human verification,\nground-truth persistence, distributed PySpark retraining, and hot-reload model serving.",
            fontsize=8.5, fontweight='bold', color='#0f172a', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/active_learning_flow.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated IEEE active_learning_flow.png (Fig. 4)")


if __name__ == "__main__":
    generate_ieee_architecture_flowchart()
    generate_ieee_nlp_pipeline()
    generate_ieee_storage_tiering()
    generate_ieee_active_learning()
    print("All IEEE standard vertical project diagrams generated successfully!")
