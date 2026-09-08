import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

os.makedirs("project_diagrams", exist_ok=True)

# Standard IEEE / ACM publication font settings
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica', 'Times New Roman']
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['figure.autolayout'] = False

# ==============================================================================
# 1. System Architecture Diagram (IEEE Single-Column Format: 3.5 in x 6.2 in)
# ==============================================================================
def draw_paper_architecture():
    # Exactly 3.6 inches wide by 6.2 inches tall - ideal for IEEE single-column
    fig, ax = plt.subplots(figsize=(3.6, 6.2), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 3.6)
    ax.set_ylim(0, 6.2)
    ax.axis('off')

    # Card Helper
    def paper_card(cx, cy, w, h, title, sub, bg, border, t_col='#0f172a'):
        shadow = FancyBboxPatch((cx - w/2 + 0.02, cy - h/2 - 0.02), w, h,
                                boxstyle="round,pad=0.03,rounding_size=0.06",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)
        card = FancyBboxPatch((cx - w/2, cy - h/2), w, h,
                              boxstyle="round,pad=0.03,rounding_size=0.06",
                              facecolor=bg, edgecolor=border, linewidth=1.2, zorder=2)
        ax.add_patch(card)
        if sub:
            ax.text(cx, cy + 0.1, title, fontsize=8.0, fontweight='bold', color=t_col, ha='center', va='center', zorder=3)
            ax.text(cx, cy - 0.1, sub, fontsize=6.5, color='#334155', ha='center', va='center', zorder=3)
        else:
            ax.text(cx, cy, title, fontsize=8.0, fontweight='bold', color=t_col, ha='center', va='center', zorder=3)

    def paper_arrow(start, end, label="", col="#475569", label_x=None, label_y=None):
        arrow = FancyArrowPatch(start, end, arrowstyle="Simple,tail_width=1.5,head_width=4.5,head_length=4.5",
                                color=col, zorder=4)
        ax.add_patch(arrow)
        if label:
            lx = label_x if label_x is not None else (start[0] + end[0]) / 2 + 0.48
            ly = label_y if label_y is not None else (start[1] + end[1]) / 2
            ax.text(lx, ly, label, fontsize=6.0, fontweight='bold', color=col, ha='center', va='center',
                    bbox=dict(boxstyle="round,pad=0.15", facecolor="#ffffff", edgecolor=col, linewidth=0.5, zorder=5))

    # Title
    ax.text(1.8, 6.02, "System Architecture Pipeline", fontsize=9.0, fontweight='bold', color='#0f172a', ha='center')

    # Step 1: Ingestion
    paper_card(1.8, 5.5, 3.3, 0.48, "1. Ingestion Layer (kafka_producer.py)", "Live Twitter Scraper (#AI, #Tech) & CSV Feed", "#f0f9ff", "#0284c7", "#0369a1")
    paper_arrow((1.8, 5.26), (1.8, 4.90), "JSON Stream", "#0284c7")

    # Step 2: Kafka Broker
    paper_card(1.8, 4.66, 3.3, 0.48, "2. Message Broker (Apache Kafka)", "Topic: 'numtest' | localhost:9092 Buffer", "#f8fafc", "#475569", "#1e293b")
    paper_arrow((1.8, 4.42), (1.8, 4.06), "Micro-Batches", "#475569")

    # Step 3: Spark Streaming
    paper_card(1.8, 3.82, 3.3, 0.48, "3. Stream Processing (PySpark)", "spark_consumer.py | Structured Streaming", "#fff7ed", "#ea580c", "#c2410c")
    paper_arrow((1.8, 3.58), (1.8, 3.22), "Sanitized Text", "#ea580c")

    # Step 4: NLP Pipeline & ML Classifier
    paper_card(1.8, 2.98, 3.3, 0.48, "4. PySpark MLlib NLP & Classifier", "Tokenizer → StopWords → TF-IDF → Logistic Reg.", "#f5f3ff", "#7c3aed", "#6d28d9")

    # Dual Storage split arrows
    paper_arrow((1.2, 2.74), (0.9, 2.36), "", "#16a34a")
    paper_arrow((2.4, 2.74), (2.7, 2.36), "", "#0891b2")
    ax.text(0.75, 2.54, "Hot", fontsize=5.8, fontweight='bold', color='#16a34a', ha='center')
    ax.text(2.85, 2.54, "Cold", fontsize=5.8, fontweight='bold', color='#0891b2', ha='center')

    # Step 5: Dual Storage Tiering
    paper_card(0.9, 2.05, 1.6, 0.55, "5a. MongoDB", "NoSQL (<50ms UI Latency)", "#f0fdf4", "#16a34a", "#15803d")
    paper_card(2.7, 2.05, 1.6, 0.55, "5b. Parquet", "Lakehouse (~82% Compressed)", "#f0f9ff", "#0891b2", "#0e7490")

    paper_arrow((0.9, 1.77), (0.9, 1.45), "", "#16a34a")

    # Step 6: Web Serving & Active Learning
    paper_card(0.9, 1.15, 1.6, 0.55, "6. Flask Web UI", "Live KPIs & Chart.js", "#fefce8", "#ca8a04", "#a16207")
    paper_card(2.7, 1.15, 1.6, 0.55, "7. Active Retrainer", "spark_retrainer.py", "#fdf2f8", "#db2777", "#be185d")

    # Feedback horizontal arrow
    ax.add_patch(FancyArrowPatch((1.7, 1.15), (1.9, 1.15), arrowstyle="Simple,tail_width=1.2,head_width=3.5,head_length=3.5", color="#db2777", zorder=4))
    ax.text(1.8, 1.32, "Feedback", fontsize=5.5, fontweight='bold', color='#db2777', ha='center')

    # Feedback loop back up to ML pipeline
    ax.add_patch(FancyArrowPatch((3.5, 1.15), (3.55, 1.15), arrowstyle="-", color="#db2777", linewidth=1.0, zorder=4))
    ax.add_patch(FancyArrowPatch((3.55, 1.15), (3.55, 2.98), arrowstyle="-", color="#db2777", linewidth=1.0, zorder=4))
    ax.add_patch(FancyArrowPatch((3.55, 2.98), (3.45, 2.98), arrowstyle="Simple,tail_width=1.2,head_width=3.5,head_length=3.5", color="#db2777", zorder=4))
    ax.text(3.55, 2.05, "Hot-Reload", fontsize=5.5, fontweight='bold', color='#db2777', ha='center', va='center', rotation=90,
            bbox=dict(boxstyle="square,pad=0.1", facecolor="#ffffff", edgecolor="#db2777", linewidth=0.5))

    # Caption
    ax.text(1.8, 0.35, "Fig. 1. End-to-end system architecture flowchart.", fontsize=7.5, fontstyle='italic', color='#475569', ha='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/architecture_diagram.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.savefig("project_diagrams/system_flowchart.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated paper-sized architecture_diagram.png")


# ==============================================================================
# 2. PySpark MLlib NLP Pipeline (IEEE Single-Column Format: 3.6 in x 5.2 in)
# ==============================================================================
def draw_paper_nlp_pipeline():
    fig, ax = plt.subplots(figsize=(3.6, 5.2), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 3.6)
    ax.set_ylim(0, 5.2)
    ax.axis('off')

    ax.text(1.8, 5.0, "PySpark MLlib NLP Pipeline", fontsize=9.0, fontweight='bold', color='#0f172a', ha='center')

    stages = [
        ("1. Raw Tweet Stream", "JSON payload (text, query keyword, timestamp)", "#f0f9ff", "#0284c7", "#0369a1", 4.50),
        ("2. Regex Text Sanitizer", "Strips URLs, Twitter handles (@), & punctuation", "#fefce8", "#d97706", "#b45309", 3.86),
        ("3. RegexTokenizer", "Converts clean text into lowercase word tokens", "#f0fdf4", "#059669", "#047857", 3.22),
        ("4. StopWordsRemover", "Removes English stopwords ('is', 'the', 'at', 'on')", "#eef2ff", "#4f46e5", "#4338ca", 2.58),
        ("5. HashingTF + IDF", "Computes 10,000-dimensional term frequency vectors", "#fdf2f8", "#db2777", "#be185d", 1.94),
        ("6. LogisticRegressionModel", "Multiclass classifier predicting probabilities", "#f5f3ff", "#7c3aed", "#6d28d9", 1.30),
        ("7. Sentiment Output", "4 Classes: Positive, Negative, Neutral, Irrelevant", "#f0fdf4", "#16a34a", "#15803d", 0.66)
    ]

    for title, desc, bg, border, t_col, y in stages:
        shadow = FancyBboxPatch((0.15 + 0.02, y - 0.20 - 0.02), 3.3, 0.42, boxstyle="round,pad=0.03,rounding_size=0.06",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        card = FancyBboxPatch((0.15, y - 0.20), 3.3, 0.42, boxstyle="round,pad=0.03,rounding_size=0.06",
                              facecolor=bg, edgecolor=border, linewidth=1.2, zorder=2)
        ax.add_patch(card)

        ax.text(1.8, y + 0.07, title, fontsize=7.8, fontweight='bold', color=t_col, ha='center', va='center', zorder=3)
        ax.text(1.8, y - 0.09, desc, fontsize=6.2, color='#334155', ha='center', va='center', zorder=3)

    for i in range(len(stages) - 1):
        y_start = stages[i][5] - 0.20
        y_end = stages[i+1][5] + 0.22
        arrow = FancyArrowPatch((1.8, y_start), (1.8, y_end),
                                arrowstyle="Simple,tail_width=1.5,head_width=4.5,head_length=4.5",
                                color="#64748b", zorder=4)
        ax.add_patch(arrow)

    ax.text(1.8, 0.16, "Fig. 2. PySpark MLlib NLP feature extraction & inference pipeline.",
            fontsize=7.2, fontstyle='italic', color='#475569', ha='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/spark_nlp_pipeline.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated paper-sized spark_nlp_pipeline.png")


# ==============================================================================
# 3. Dual Storage Tiering (IEEE Single-Column Format: 3.6 in x 4.2 in)
# ==============================================================================
def draw_paper_storage_tiering():
    fig, ax = plt.subplots(figsize=(3.6, 4.2), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 3.6)
    ax.set_ylim(0, 4.2)
    ax.axis('off')

    ax.text(1.8, 4.0, "Storage Architecture: Hot vs. Cold", fontsize=9.0, fontweight='bold', color='#0f172a', ha='center')

    # Stream Producer
    card_in = FancyBboxPatch((0.15, 3.25), 3.3, 0.45, boxstyle="round,pad=0.03,rounding_size=0.06",
                             facecolor='#fff7ed', edgecolor='#ea580c', linewidth=1.2, zorder=2)
    ax.add_patch(card_in)
    ax.text(1.8, 3.52, "PySpark Streaming Ingestion", fontsize=8.0, fontweight='bold', color='#c2410c', ha='center')
    ax.text(1.8, 3.34, "Concurrent Dual-Sink Real-Time Persistence", fontsize=6.2, color='#334155', ha='center')

    # Branches
    ax.add_patch(FancyArrowPatch((1.0, 3.25), (0.9, 2.75), arrowstyle="Simple,tail_width=1.2,head_width=4,head_length=4", color="#16a34a", zorder=3))
    ax.add_patch(FancyArrowPatch((2.6, 3.25), (2.7, 2.75), arrowstyle="Simple,tail_width=1.2,head_width=4,head_length=4", color="#0891b2", zorder=3))
    ax.text(0.65, 3.0, "Hot Path", fontsize=6.0, fontweight='bold', color='#16a34a', ha='center')
    ax.text(2.95, 3.0, "Cold Path", fontsize=6.0, fontweight='bold', color='#0891b2', ha='center')

    # Storage Sinks
    card_hot = FancyBboxPatch((0.15, 1.95), 1.55, 0.75, boxstyle="round,pad=0.03,rounding_size=0.06",
                              facecolor='#f0fdf4', edgecolor='#16a34a', linewidth=1.2, zorder=2)
    ax.add_patch(card_hot)
    ax.text(0.925, 2.52, "Hot: MongoDB NoSQL", fontsize=7.5, fontweight='bold', color='#15803d', ha='center')
    ax.text(0.925, 2.20, "• JSON Docs\n• <50ms Query Latency\n• Feeds Live UI", fontsize=6.0, color='#334155', ha='center')

    card_cold = FancyBboxPatch((1.9, 1.95), 1.55, 0.75, boxstyle="round,pad=0.03,rounding_size=0.06",
                               facecolor='#f0f9ff', edgecolor='#0891b2', linewidth=1.2, zorder=2)
    ax.add_patch(card_cold)
    ax.text(2.675, 2.52, "Cold: Parquet Lakehouse", fontsize=7.5, fontweight='bold', color='#0e7490', ha='center')
    ax.text(2.675, 2.20, "• Snappy Columnar\n• ~82% Disk Saved\n• Spark Retraining", fontsize=6.0, color='#334155', ha='center')

    # Sinks
    ax.add_patch(FancyArrowPatch((0.925, 1.95), (0.925, 1.45), arrowstyle="Simple,tail_width=1.2,head_width=4,head_length=4", color="#16a34a", zorder=3))
    ax.add_patch(FancyArrowPatch((2.675, 1.95), (2.675, 1.45), arrowstyle="Simple,tail_width=1.2,head_width=4,head_length=4", color="#0891b2", zorder=3))

    card_ui = FancyBboxPatch((0.15, 0.90), 1.55, 0.50, boxstyle="round,pad=0.03,rounding_size=0.06",
                             facecolor='#fefce8', edgecolor='#ca8a04', linewidth=1.2, zorder=2)
    ax.add_patch(card_ui)
    ax.text(0.925, 1.20, "Flask Dashboard", fontsize=7.5, fontweight='bold', color='#a16207', ha='center')
    ax.text(0.925, 1.00, "Real-Time Telemetry", fontsize=6.0, color='#334155', ha='center')

    card_retrain = FancyBboxPatch((1.9, 0.90), 1.55, 0.50, boxstyle="round,pad=0.03,rounding_size=0.06",
                                  facecolor='#fdf2f8', edgecolor='#db2777', linewidth=1.2, zorder=2)
    ax.add_patch(card_retrain)
    ax.text(2.675, 1.20, "PySpark Retraining", fontsize=7.5, fontweight='bold', color='#be185d', ha='center')
    ax.text(2.675, 1.00, "Batch Analytics Scan", fontsize=6.0, color='#334155', ha='center')

    ax.text(1.8, 0.30, "Fig. 3. Dual storage tiering: Hot MongoDB vs. Cold Parquet.", fontsize=7.2, fontstyle='italic', color='#475569', ha='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/lakehouse_tiering.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated paper-sized lakehouse_tiering.png")


# ==============================================================================
# 4. Human-in-the-Loop Active Learning (IEEE Single-Column Format: 3.6 in x 4.8 in)
# ==============================================================================
def draw_paper_active_learning():
    fig, ax = plt.subplots(figsize=(3.6, 4.8), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 3.6)
    ax.set_ylim(0, 4.8)
    ax.axis('off')

    ax.text(1.8, 4.6, "Active Learning Retraining Loop", fontsize=9.0, fontweight='bold', color='#0f172a', ha='center')

    steps = [
        ("Step 1: Real-Time Tweet Inspection", "Operator monitors live stream & model predictions on UI", "#f0f9ff", "#0284c7", "#0369a1", 4.05),
        ("Step 2: Interactive Label Correction", "User clicks correction button on misclassified tweets", "#fdf2f8", "#db2777", "#be185d", 3.25),
        ("Step 3: Ground-Truth MongoDB Sync", "Updates record to 'is_verified: true' with target label", "#f0fdf4", "#16a34a", "#15803d", 2.45),
        ("Step 4: PySpark Batch Retraining", "spark_retrainer.py fits MLlib pipeline on merged data", "#fff7ed", "#ea580c", "#c2410c", 1.65),
        ("Step 5: Dynamic Model Hot-Reload", "Updated PipelineModel reloads into streaming engine", "#f5f3ff", "#7c3aed", "#6d28d9", 0.85)
    ]

    for title, desc, bg, border, t_col, y in steps:
        shadow = FancyBboxPatch((0.15 + 0.02, y - 0.22 - 0.02), 3.3, 0.44, boxstyle="round,pad=0.03,rounding_size=0.06",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        card = FancyBboxPatch((0.15, y - 0.22), 3.3, 0.44, boxstyle="round,pad=0.03,rounding_size=0.06",
                              facecolor=bg, edgecolor=border, linewidth=1.2, zorder=2)
        ax.add_patch(card)

        ax.text(1.8, y + 0.08, title, fontsize=7.8, fontweight='bold', color=t_col, ha='center', va='center', zorder=3)
        ax.text(1.8, y - 0.09, desc, fontsize=6.2, color='#334155', ha='center', va='center', zorder=3)

    for i in range(len(steps) - 1):
        y_start = steps[i][5] - 0.22
        y_end = stages_y_end = steps[i+1][5] + 0.24
        arrow = FancyArrowPatch((1.8, y_start), (1.8, y_end),
                                arrowstyle="Simple,tail_width=1.5,head_width=4.5,head_length=4.5",
                                color="#64748b", zorder=4)
        ax.add_patch(arrow)

    # Active Feedback Loop line back to top
    ax.add_patch(FancyArrowPatch((3.45, 0.85), (3.55, 0.85), arrowstyle="-", color="#7c3aed", linewidth=1.0, zorder=4))
    ax.add_patch(FancyArrowPatch((3.55, 0.85), (3.55, 4.05), arrowstyle="-", color="#7c3aed", linewidth=1.0, zorder=4))
    ax.add_patch(FancyArrowPatch((3.55, 4.05), (3.45, 4.05), arrowstyle="Simple,tail_width=1.2,head_width=4,head_length=4", color="#7c3aed", zorder=4))
    ax.text(3.55, 2.45, "Continuous Adaptation Loop", fontsize=5.8, fontweight='bold', color='#7c3aed',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="square,pad=0.1", facecolor="#ffffff", edgecolor="#7c3aed", linewidth=0.5))

    ax.text(1.8, 0.22, "Fig. 4. Closed-loop active learning and continuous model retraining.", fontsize=7.2, fontstyle='italic', color='#475569', ha='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/active_learning_flow.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated paper-sized active_learning_flow.png")


if __name__ == "__main__":
    draw_paper_architecture()
    draw_paper_nlp_pipeline()
    draw_paper_storage_tiering()
    draw_paper_active_learning()
    print("All research-paper-fitted diagrams generated successfully at 300 DPI!")
