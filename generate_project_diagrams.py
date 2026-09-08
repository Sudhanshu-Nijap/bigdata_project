import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Polygon

os.makedirs("project_diagrams", exist_ok=True)

plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['font.family'] = 'sans-serif'

# =============================================================
# 1. Main System Architecture Vertical Flowchart (White Theme)
# =============================================================
def generate_architecture_diagram():
    fig, ax = plt.subplots(figsize=(11, 17), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 17)
    ax.axis('off')

    # Main Header
    ax.text(5.5, 16.5, "Real-Time Twitter Sentiment Analysis Pipeline",
            fontsize=17, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(5.5, 16.15, "End-to-End Distributed Big Data Architecture • Vertical Flowchart",
            fontsize=10.5, color='#64748b', ha='center', va='center')

    # Helper: Process Block
    def draw_node(x, y, w, h, title, subtitle="", items=[], bg="#f8fafc", border="#3b82f6", tag=""):
        # Shadow
        shadow = FancyBboxPatch((x+0.04, y-0.04), w, h, boxstyle="round,pad=0.08,rounding_size=0.15",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        # Box
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08,rounding_size=0.15",
                             facecolor=bg, edgecolor=border, linewidth=2, zorder=2)
        ax.add_patch(box)

        # Tag
        if tag:
            t_len = len(tag) * 0.12 + 0.3
            t_box = FancyBboxPatch((x + 0.15, y + h - 0.26), t_len, 0.28,
                                   boxstyle="round,pad=0.03,rounding_size=0.06",
                                   facecolor=border, edgecolor='none', zorder=3)
            ax.add_patch(t_box)
            ax.text(x + 0.15 + t_len/2, y + h - 0.12, tag, fontsize=7, fontweight='bold',
                    color='#ffffff', ha='center', va='center', zorder=4)

        # Title
        ax.text(x + w/2, y + h - 0.42, title, fontsize=11, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=3)

        # Subtitle & items
        cur_y = y + h - 0.72
        if subtitle:
            ax.text(x + w/2, cur_y, subtitle, fontsize=8.5, color='#64748b', ha='center', va='center', style='italic', zorder=3)
            cur_y -= 0.35

        for itm in items:
            ax.text(x + 0.3, cur_y, f"• {itm}", fontsize=8, color='#334155', va='center', zorder=3)
            cur_y -= 0.26

    # Helper: Decision Diamond
    def draw_decision(cx, cy, r_w, r_h, title, subtitle="", bg="#fdf2f8", border="#db2777"):
        pts = [[cx, cy + r_h], [cx + r_w, cy], [cx, cy - r_h], [cx - r_w, cy]]
        shadow = Polygon([[p[0]+0.04, p[1]-0.04] for p in pts], closed=True, facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)
        poly = Polygon(pts, closed=True, facecolor=bg, edgecolor=border, linewidth=2, zorder=2)
        ax.add_patch(poly)
        ax.text(cx, cy + 0.15, title, fontsize=10, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=3)
        if subtitle:
            ax.text(cx, cy - 0.2, subtitle, fontsize=7.5, color='#475569', ha='center', va='center', zorder=3)

    # Helper: Downward Arrow
    def draw_v_arrow(start, end, label="", col="#475569", label_side="right"):
        arrow = FancyArrowPatch(start, end, arrowstyle="Simple,tail_width=2.5,head_width=8,head_length=8", color=col, zorder=4)
        ax.add_patch(arrow)
        if label:
            mx = (start[0] + end[0]) / 2 + (0.95 if label_side == "right" else -0.95)
            my = (start[1] + end[1]) / 2
            ax.text(mx, my, label, fontsize=7.5, fontweight='bold', color=col, ha='center', va='center',
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="#ffffff", edgecolor=col, linewidth=0.8, zorder=5))

    # --- Step 1: Twitter Ingestion ---
    draw_node(2.2, 14.5, 6.6, 1.3, "1. Data Ingestion (Twitter Stream)", "kafka_producer.py",
              ["Live Scraper (#AI, #Crypto, #Tech) & CSV validation dataset"],
              "#f0f9ff", "#0284c7", "INGESTION")

    draw_v_arrow((5.5, 14.5), (5.5, 13.5), "JSON Messages", "#0284c7")

    # --- Step 2: Kafka Distributed Broker ---
    draw_node(2.2, 12.2, 6.6, 1.3, "2. Message Broker (Apache Kafka)", "kafka_producer.py -> localhost:9092",
              ["Topic: 'numtest' | Partitioned Buffer | In-Memory Streaming Queue"],
              "#f8fafc", "#475569", "BROKER")

    draw_v_arrow((5.5, 12.2), (5.5, 11.2), "Distributed Stream", "#475569")

    # --- Step 3: PySpark Streaming Consumer ---
    draw_node(2.2, 9.9, 6.6, 1.3, "3. Stream Processing (PySpark)", "spark_consumer.py",
              ["PySpark Structured Streaming consumer reading micro-batches"],
              "#fff7ed", "#ea580c", "SPEED LAYER")

    draw_v_arrow((5.5, 9.9), (5.5, 8.9), "Preprocess Text", "#ea580c")

    # --- Step 4: NLP Pipeline ---
    draw_node(2.2, 7.6, 6.6, 1.3, "4. PySpark MLlib NLP Pipeline", "spark_pipeline_artifact",
              ["Regex Sanitizer → Tokenizer → StopWordsRemover → HashingTF + IDF"],
              "#fff7ed", "#ea580c", "NLP PIPELINE")

    draw_v_arrow((5.5, 7.6), (5.5, 6.6), "Feature Vectors", "#ea580c")

    # --- Step 5: MLlib Logistic Regression Classifier & Decision ---
    draw_decision(5.5, 5.8, 2.5, 0.8, "5. MLlib Classification", "Pos / Neg / Neu / Irr", "#fdf2f8", "#db2777")

    draw_v_arrow((5.5, 5.0), (5.5, 4.2), "Predictions", "#db2777")

    # --- Step 6: Dual Storage Tiering ---
    draw_node(0.8, 2.7, 4.4, 1.5, "6a. Hot Store: MongoDB", "Operational Sink",
              ["Uncompressed JSON Docs", "Sub-second UI query latency"],
              "#f0fdf4", "#16a34a", "HOT TIER")

    draw_node(5.8, 2.7, 4.4, 1.5, "6b. Cold Store: Parquet", "Data Lakehouse Sink",
              ["Snappy Columnar Format", "~82% Storage Space Saved"],
              "#f0f9ff", "#0891b2", "COLD TIER")

    # Connect decision diamond to both storage boxes
    ax.add_patch(FancyArrowPatch((4.2, 5.1), (3.0, 4.2), arrowstyle="Simple,tail_width=2,head_width=6,head_length=6", color="#16a34a", zorder=4))
    ax.add_patch(FancyArrowPatch((6.8, 5.1), (8.0, 4.2), arrowstyle="Simple,tail_width=2,head_width=6,head_length=6", color="#0891b2", zorder=4))

    # --- Step 7: Web Dashboard Serving ---
    draw_node(2.2, 0.6, 6.6, 1.5, "7. Serving Dashboard (Flask Web UI)", "app.py (Port 8000)",
              ["Real-Time Sentiment Metrics • Chart.js Graphs • 1-Click Parquet Export"],
              "#fefce8", "#ca8a04", "SERVING")

    # Arrow from MongoDB to Dashboard
    draw_v_arrow((3.0, 2.7), (3.0, 2.1), "Live Queries", "#16a34a", label_side="left")

    # --- Feedback Loop: Dashboard -> Retrainer -> Spark ML ---
    draw_node(6.8, 1.2, 3.8, 1.1, "8. Active Learning", "spark_retrainer.py",
              ["Human Feedback → Retrain Pipeline"],
              "#fdf2f8", "#db2777", "RETRAIN")

    # Arrow from UI to Active Learning
    ax.add_patch(FancyArrowPatch((5.5, 0.6), (5.5, 0.3), arrowstyle="-", color="#db2777", linewidth=2, zorder=4))
    ax.add_patch(FancyArrowPatch((5.5, 0.3), (8.7, 0.3), arrowstyle="-", color="#db2777", linewidth=2, zorder=4))
    ax.add_patch(FancyArrowPatch((8.7, 0.3), (8.7, 1.2), arrowstyle="Simple,tail_width=2,head_width=6,head_length=6", color="#db2777", zorder=4))
    ax.text(7.1, 0.15, "User Label Corrections", fontsize=7.5, fontweight='bold', color='#db2777', ha='center', va='center')

    # Arrow from Retrainer back up to PySpark NLP Pipeline
    ax.add_patch(FancyArrowPatch((8.7, 2.3), (10.2, 2.3), arrowstyle="-", color="#db2777", linewidth=2, zorder=4))
    ax.add_patch(FancyArrowPatch((10.2, 2.3), (10.2, 8.2), arrowstyle="-", color="#db2777", linewidth=2, zorder=4))
    ax.add_patch(FancyArrowPatch((10.2, 8.2), (8.8, 8.2), arrowstyle="Simple,tail_width=2,head_width=6,head_length=6", color="#db2777", zorder=4))
    ax.text(10.2, 5.5, "Reload Retrained Model Artifact", fontsize=7.5, fontweight='bold', color='#db2777',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="round,pad=0.2", facecolor="#ffffff", edgecolor="#db2777", linewidth=0.8))

    plt.tight_layout()
    plt.savefig("project_diagrams/architecture_diagram.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.savefig("project_diagrams/system_flowchart.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated vertical architecture_diagram.png & system_flowchart.png (White Theme)")


# =============================================================
# 2. PySpark MLlib NLP Pipeline (Vertical Flowchart)
# =============================================================
def generate_nlp_pipeline_diagram():
    fig, ax = plt.subplots(figsize=(9, 13), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 9)
    ax.set_ylim(0, 13)
    ax.axis('off')

    ax.text(4.5, 12.5, "PySpark MLlib NLP Feature & Inference Pipeline",
            fontsize=15, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.5, 12.15, "Vertical Stage-by-Stage Flow (spark_pipeline_artifact)",
            fontsize=9.5, color='#64748b', ha='center', va='center')

    stages = [
        ("1. Raw Tweet Stream", "Input Text (Hashtags, User Mentions, Web URLs)", "#0284c7", "#f0f9ff", 10.7),
        ("2. Regex Text Sanitizer", "Strips URLs, Twitter Handles (@), & Special Symbols", "#d97706", "#fefce8", 9.1),
        ("3. RegexTokenizer", "Splits Sanitized Text Into Lowercase Word Tokens", "#059669", "#f0fdf4", 7.5),
        ("4. StopWordsRemover", "Eliminates Common English Stopwords ('is', 'the', 'at')", "#4f46e5", "#eef2ff", 5.9),
        ("5. HashingTF + IDF", "Generates 10,000-Dimensional Term Frequency Feature Vectors", "#db2777", "#fdf2f8", 4.3),
        ("6. LogisticRegressionModel", "PySpark MLlib Multiclass Probability Classifier", "#7c3aed", "#f5f3ff", 2.7),
        ("7. Sentiment Output", "4 Classes: Positive, Negative, Neutral, Irrelevant", "#16a34a", "#f0fdf4", 1.1)
    ]

    for title, desc, col, bg, y in stages:
        shadow = FancyBboxPatch((1.54, y-0.04), 5.92, 1.1, boxstyle="round,pad=0.08,rounding_size=0.15",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        box = FancyBboxPatch((1.5, y), 6.0, 1.1, boxstyle="round,pad=0.08,rounding_size=0.15",
                             facecolor=bg, edgecolor=col, linewidth=2, zorder=2)
        ax.add_patch(box)
        ax.text(4.5, y + 0.72, title, fontsize=10.5, fontweight='bold', color='#0f172a', ha='center', zorder=3)
        ax.text(4.5, y + 0.32, desc, fontsize=8.5, color='#334155', ha='center', zorder=3)

    for i in range(len(stages) - 1):
        y_start = stages[i][4]
        y_end = stages[i+1][4] + 1.1
        arrow = FancyArrowPatch((4.5, y_start), (4.5, y_end),
                                arrowstyle="Simple,tail_width=2.5,head_width=7,head_length=7",
                                color="#64748b", zorder=4)
        ax.add_patch(arrow)

    plt.tight_layout()
    plt.savefig("project_diagrams/spark_nlp_pipeline.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated vertical spark_nlp_pipeline.png (White Theme)")


# =============================================================
# 3. Storage Tiering Flow (White Theme)
# =============================================================
def generate_storage_tiering_diagram():
    fig, ax = plt.subplots(figsize=(12, 7), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 7)
    ax.axis('off')

    ax.text(6, 6.5, "Big Data Storage Architecture: Hot Tier vs. Cold Lakehouse",
            fontsize=15, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(6, 6.1, "Optimized for High Write-Throughput, Sub-Second UI Latency & ~82% Parquet Compression",
            fontsize=9.5, color='#64748b', ha='center', va='center')

    # Stream Ingestion
    box_in = FancyBboxPatch((0.5, 2.0), 3.0, 3.2, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#fff7ed', edgecolor='#ea580c', linewidth=2, zorder=2)
    ax.add_patch(box_in)
    ax.text(2.0, 4.6, "PySpark Stream", fontsize=11, fontweight='bold', color='#0f172a', ha='center')
    ax.text(2.0, 3.3, "Continuous Real-Time\nIngestion & Sentiment\nInference Pipeline\n(Micro-batches)",
            fontsize=8.5, color='#334155', ha='center')

    # Hot Tier
    box_hot = FancyBboxPatch((4.4, 3.4), 3.8, 2.0, boxstyle="round,pad=0.1,rounding_size=0.15",
                             facecolor='#f0fdf4', edgecolor='#16a34a', linewidth=2, zorder=2)
    ax.add_patch(box_hot)
    ax.text(6.3, 4.9, "Hot Tier: MongoDB NoSQL", fontsize=10.5, fontweight='bold', color='#16a34a', ha='center')
    ax.text(6.3, 4.0, "• Raw JSON document format\n• Sub-second query response time\n• Feeds live teleprinter & real-time UI",
            fontsize=8, color='#334155', ha='center')

    # Cold Tier
    box_cold = FancyBboxPatch((4.4, 0.8), 3.8, 2.0, boxstyle="round,pad=0.1,rounding_size=0.15",
                              facecolor='#f0f9ff', edgecolor='#0891b2', linewidth=2, zorder=2)
    ax.add_patch(box_cold)
    ax.text(6.3, 2.3, "Cold Tier: Parquet Lakehouse", fontsize=10.5, fontweight='bold', color='#0891b2', ha='center')
    ax.text(6.3, 1.4, "• Snappy Columnar Storage\n• ~82% Storage Space Reduction\n• Fast OLAP Scans & Predicate Pushdown",
            fontsize=8, color='#334155', ha='center')

    # Serving Targets
    box_ui = FancyBboxPatch((9.0, 3.4), 2.5, 2.0, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#fefce8', edgecolor='#ca8a04', linewidth=2, zorder=2)
    ax.add_patch(box_ui)
    ax.text(10.25, 4.9, "Live UI Serving", fontsize=10.5, fontweight='bold', color='#0f172a', ha='center')
    ax.text(10.25, 4.0, "• Flask Web Server\n• Live Charts & KPIs\n• User Verifications", fontsize=8, color='#334155', ha='center')

    box_batch = FancyBboxPatch((9.0, 0.8), 2.5, 2.0, boxstyle="round,pad=0.1,rounding_size=0.15",
                               facecolor='#fdf2f8', edgecolor='#db2777', linewidth=2, zorder=2)
    ax.add_patch(box_batch)
    ax.text(10.25, 2.3, "Batch Analytics", fontsize=10.5, fontweight='bold', color='#0f172a', ha='center')
    ax.text(10.25, 1.4, "• PySpark Retraining\n• Long-term Trends\n• Column Queries", fontsize=8, color='#334155', ha='center')

    def draw_a(p1, p2, col):
        ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle="Simple,tail_width=2,head_width=7,head_length=7", color=col, zorder=3))

    draw_a((3.5, 4.4), (4.4, 4.4), "#16a34a")
    draw_a((3.5, 2.6), (4.4, 1.8), "#0891b2")
    draw_a((8.2, 4.4), (9.0, 4.4), "#ca8a04")
    draw_a((8.2, 1.8), (9.0, 1.8), "#db2777")

    plt.tight_layout()
    plt.savefig("project_diagrams/lakehouse_tiering.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated lakehouse_tiering.png (White Theme)")


# =============================================================
# 4. Human-in-the-Loop Active Learning Vertical Flow (White Theme)
# =============================================================
def generate_active_learning_diagram():
    fig, ax = plt.subplots(figsize=(9, 11), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 9)
    ax.set_ylim(0, 11)
    ax.axis('off')

    ax.text(4.5, 10.5, "Human-in-the-Loop Active Learning Flowchart",
            fontsize=15, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.5, 10.15, "Continuous Model Evolution via Real-Time User Feedback Loop",
            fontsize=9.5, color='#64748b', ha='center', va='center')

    steps = [
        ("1. Streaming Live Feed", "User inspects live tweet stream & predictions on Flask UI", "#0284c7", "#f0f9ff", 8.8),
        ("2. Human Verification", "User clicks correction buttons (+Pos, -Neg, Neu, Irr)", "#db2777", "#fdf2f8", 6.8),
        ("3. MongoDB Ground Truth Sync", "Updates document with 'is_verified: true' & verified class", "#16a34a", "#f0fdf4", 4.8),
        ("4. PySpark Batch Retrainer", "Merges verified feedback with baseline & retrains MLlib pipeline", "#ea580c", "#fff7ed", 2.8),
        ("5. Live Model Hot-Reload", "Overwrites artifact & production inference dynamically updates", "#7c3aed", "#f5f3ff", 0.8)
    ]

    for title, desc, col, bg, y in steps:
        shadow = FancyBboxPatch((1.54, y-0.04), 5.92, 1.2, boxstyle="round,pad=0.08,rounding_size=0.15",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        box = FancyBboxPatch((1.5, y), 6.0, 1.2, boxstyle="round,pad=0.08,rounding_size=0.15",
                             facecolor=bg, edgecolor=col, linewidth=2, zorder=2)
        ax.add_patch(box)
        ax.text(4.5, y + 0.8, title, fontsize=10.5, fontweight='bold', color='#0f172a', ha='center', zorder=3)
        ax.text(4.5, y + 0.38, desc, fontsize=8.5, color='#334155', ha='center', zorder=3)

    for i in range(len(steps) - 1):
        y_start = steps[i][4]
        y_end = steps[i+1][4] + 1.2
        arrow = FancyArrowPatch((4.5, y_start), (4.5, y_end),
                                arrowstyle="Simple,tail_width=2.5,head_width=7,head_length=7",
                                color="#64748b", zorder=4)
        ax.add_patch(arrow)

    # Feedback loop arrow back to top
    ax.add_patch(FancyArrowPatch((7.5, 1.4), (8.2, 1.4), arrowstyle="-", color="#7c3aed", linewidth=2, zorder=4))
    ax.add_patch(FancyArrowPatch((8.2, 1.4), (8.2, 9.4), arrowstyle="-", color="#7c3aed", linewidth=2, zorder=4))
    ax.add_patch(FancyArrowPatch((8.2, 9.4), (7.5, 9.4), arrowstyle="Simple,tail_width=2.5,head_width=7,head_length=7", color="#7c3aed", zorder=4))
    ax.text(8.2, 5.4, "Continuous Active Feedback Loop", fontsize=8, fontweight='bold', color='#7c3aed',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="round,pad=0.2", facecolor="#ffffff", edgecolor="#7c3aed", linewidth=0.8))

    plt.tight_layout()
    plt.savefig("project_diagrams/active_learning_flow.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated vertical active_learning_flow.png (White Theme)")

if __name__ == "__main__":
    generate_architecture_diagram()
    generate_nlp_pipeline_diagram()
    generate_storage_tiering_diagram()
    generate_active_learning_diagram()
    print("All Vertical White Theme flow diagrams generated successfully!")
