import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

os.makedirs("project_diagrams", exist_ok=True)

# Clean, professional sans-serif typography
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']
plt.rcParams['font.family'] = 'sans-serif'

# ==============================================================================
# 1. Main System Architecture Flowchart (Ultra Clean & Neat)
# ==============================================================================
def draw_neat_architecture():
    fig, ax = plt.subplots(figsize=(10.5, 16.5), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 10.5)
    ax.set_ylim(0, 16.5)
    ax.axis('off')

    # Main Title Header
    ax.text(5.25, 15.9, "Real-Time Twitter Sentiment Analysis Pipeline",
            fontsize=16.5, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(5.25, 15.45, "End-to-End Distributed Big Data Architecture & Sequential Flowchart",
            fontsize=10.5, color='#475569', ha='center', va='center')

    # Box Drawing Helper
    def make_box(cx, cy, w, h, title, subtitle, bg, border, title_col='#0f172a'):
        # Subtle Drop Shadow
        shadow = FancyBboxPatch((cx - w/2 + 0.05, cy - h/2 - 0.05), w, h,
                                boxstyle="round,pad=0.08,rounding_size=0.12",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        # Main Card Box
        card = FancyBboxPatch((cx - w/2, cy - h/2), w, h,
                              boxstyle="round,pad=0.08,rounding_size=0.12",
                              facecolor=bg, edgecolor=border, linewidth=2.0, zorder=2)
        ax.add_patch(card)

        # Title
        ax.text(cx, cy + 0.22, title, fontsize=11.5, fontweight='bold', color=title_col, ha='center', va='center', zorder=3)
        # Subtitle
        ax.text(cx, cy - 0.22, subtitle, fontsize=9.0, color='#334155', ha='center', va='center', zorder=3)

    # Clean Arrow Helper
    def make_arrow(start, end, label="", col="#475569", label_side="right"):
        arrow = FancyArrowPatch(start, end, arrowstyle="Simple,tail_width=2.5,head_width=7.5,head_length=7.5",
                                color=col, zorder=4)
        ax.add_patch(arrow)
        if label:
            mx = (start[0] + end[0]) / 2 + (1.2 if label_side == "right" else -1.2)
            my = (start[1] + end[1]) / 2
            ax.text(mx, my, label, fontsize=8.0, fontweight='bold', color=col, ha='center', va='center',
                    bbox=dict(boxstyle="round,pad=0.25", facecolor="#ffffff", edgecolor=col, linewidth=0.8, zorder=5))

    # --- Node 1: Ingestion ---
    make_box(5.25, 14.2, 8.0, 1.15, "1. Live Data Ingestion Layer",
             "kafka_producer.py  •  Twitter Scraper (#AI, #Tech, #Crypto) & CSV Stream",
             "#f0f9ff", "#0284c7", "#0369a1")
    make_arrow((5.25, 13.62), (5.25, 12.68), "JSON Payloads", "#0284c7")

    # --- Node 2: Kafka Broker ---
    make_box(5.25, 12.1, 8.0, 1.15, "2. Distributed Message Broker (Apache Kafka)",
             "Topic: 'numtest'  •  localhost:9092  •  In-Memory Partition Buffer",
             "#f8fafc", "#475569", "#1e293b")
    make_arrow((5.25, 11.52), (5.25, 10.58), "Streaming Micro-Batches", "#475569")

    # --- Node 3: PySpark Streaming ---
    make_box(5.25, 10.0, 8.0, 1.15, "3. Stream Processing Engine (PySpark)",
             "spark_consumer.py  •  Structured Streaming Micro-Batch Consumer",
             "#fff7ed", "#ea580c", "#c2410c")
    make_arrow((5.25, 9.42), (5.25, 8.48), "Preprocessed Text", "#ea580c")

    # --- Node 4: NLP Pipeline ---
    make_box(5.25, 7.9, 8.0, 1.15, "4. PySpark MLlib NLP Transformation Pipeline",
             "Regex Cleaner  →  RegexTokenizer  →  StopWordsRemover  →  HashingTF + IDF",
             "#f5f3ff", "#7c3aed", "#6d28d9")
    make_arrow((5.25, 7.32), (5.25, 6.38), "10,000-Dim TF-IDF Vectors", "#7c3aed")

    # --- Node 5: ML Classifier Decision ---
    make_box(5.25, 5.8, 8.0, 1.15, "5. MLlib Multiclass Classification",
             "LogisticRegressionModel  •  Classes: Positive, Negative, Neutral, Irrelevant",
             "#fdf2f8", "#db2777", "#be185d")

    # Dual Storage Arrows
    make_arrow((3.8, 5.22), (2.8, 4.38), "Hot Persistence", "#16a34a", label_side="left")
    make_arrow((6.7, 5.22), (7.7, 4.38), "Cold Archival", "#0891b2", label_side="right")

    # --- Node 6: Dual Storage Tiering (Side by Side) ---
    make_box(2.8, 3.75, 4.2, 1.25, "6a. Hot Operational Store",
             "MongoDB NoSQL\nSub-second live UI query response",
             "#f0fdf4", "#16a34a", "#15803d")

    make_box(7.7, 3.75, 4.2, 1.25, "6b. Cold Data Lakehouse",
             "Apache Parquet (Snappy)\n~82% disk reduction • Fast OLAP scans",
             "#f0f9ff", "#0891b2", "#0e7490")

    # Arrow from MongoDB down to Flask
    make_arrow((2.8, 3.12), (2.8, 2.18), "Live Feed", "#16a34a", label_side="left")

    # --- Node 7: Web Dashboard & Active Learning ---
    make_box(2.8, 1.55, 4.2, 1.25, "7. Serving Dashboard (Flask)",
             "webapp/app.py (Port 8000)\nLive Sentiment KPIs, Charts & Teleprinter",
             "#fefce8", "#ca8a04", "#a16207")

    make_box(7.7, 1.55, 4.2, 1.25, "8. Active Learning Retrainer",
             "spark_retrainer.py\nGround-Truth feedback & batch retraining",
             "#fdf2f8", "#db2777", "#be185d")

    # User Feedback Arrow (Flask UI -> Retrainer)
    ax.add_patch(FancyArrowPatch((4.9, 1.55), (5.6, 1.55),
                                arrowstyle="Simple,tail_width=2.5,head_width=7,head_length=7",
                                color="#db2777", zorder=4))
    ax.text(5.25, 1.95, "User Corrections", fontsize=8.0, fontweight='bold', color='#db2777', ha='center', va='center',
            bbox=dict(boxstyle="round,pad=0.25", facecolor="#ffffff", edgecolor="#db2777", linewidth=0.8, zorder=5))

    # Feedback loop up back to Spark ML pipeline
    ax.add_patch(FancyArrowPatch((9.8, 1.55), (10.15, 1.55), arrowstyle="-", color="#db2777", linewidth=2.0, zorder=4))
    ax.add_patch(FancyArrowPatch((10.15, 1.55), (10.15, 7.9), arrowstyle="-", color="#db2777", linewidth=2.0, zorder=4))
    ax.add_patch(FancyArrowPatch((10.15, 7.9), (9.25, 7.9), arrowstyle="Simple,tail_width=2.5,head_width=7,head_length=7", color="#db2777", zorder=4))
    ax.text(10.15, 4.8, "Hot-Reload Retrained ML Model Artifact", fontsize=8.0, fontweight='bold', color='#db2777',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="round,pad=0.25", facecolor="#ffffff", edgecolor="#db2777", linewidth=0.8, zorder=5))

    # Caption
    ax.text(5.25, 0.45, "Fig. 1. End-to-end vertical architectural flowchart of the real-time distributed Twitter sentiment analysis pipeline.",
            fontsize=9.0, fontstyle='italic', color='#64748b', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/architecture_diagram.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.savefig("project_diagrams/system_flowchart.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated clean architecture_diagram.png & system_flowchart.png")


# ==============================================================================
# 2. PySpark MLlib NLP Pipeline (Neat & High Legibility)
# ==============================================================================
def draw_neat_nlp_pipeline():
    fig, ax = plt.subplots(figsize=(9.5, 13.5), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 9.5)
    ax.set_ylim(0, 13.5)
    ax.axis('off')

    ax.text(4.75, 12.8, "PySpark MLlib NLP Feature & Inference Pipeline",
            fontsize=15.5, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.75, 12.4, "Sequential Natural Language Processing Stages (spark_pipeline_artifact)",
            fontsize=10.0, color='#475569', ha='center', va='center')

    stages = [
        ("1. Raw Tweet Ingestion", "JSON Payload containing raw tweet text, author handle, & timestamp", "#f0f9ff", "#0284c7", "#0369a1", 11.0),
        ("2. Regex Text Sanitizer", "Strips HTTP/HTTPS URLs, Twitter @mentions, punctuation & emojis", "#fefce8", "#d97706", "#b45309", 9.4),
        ("3. RegexTokenizer", "Transforms cleaned string into normalized lowercase word token array", "#f0fdf4", "#059669", "#047857", 7.8),
        ("4. StopWordsRemover", "Filters out high-frequency uninformative stopwords ('is', 'the', 'at', 'on')", "#eef2ff", "#4f46e5", "#4338ca", 6.2),
        ("5. HashingTF + IDF", "Maps tokens into 10,000-dimensional term frequency-inverse document vector space", "#fdf2f8", "#db2777", "#be185d", 4.6),
        ("6. LogisticRegressionModel", "PySpark MLlib Multiclass Logistic Regression classifier evaluating probabilities", "#f5f3ff", "#7c3aed", "#6d28d9", 3.0),
        ("7. Sentiment Output", "Final 4-class classification: Positive (1), Negative (0), Neutral (2), Irrelevant (3)", "#f0fdf4", "#16a34a", "#15803d", 1.4)
    ]

    for title, desc, bg, border, t_col, y in stages:
        shadow = FancyBboxPatch((1.05, y - 0.52), 7.4, 1.04, boxstyle="round,pad=0.08,rounding_size=0.12",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        card = FancyBboxPatch((1.0, y - 0.48), 7.5, 1.04, boxstyle="round,pad=0.08,rounding_size=0.12",
                              facecolor=bg, edgecolor=border, linewidth=2.0, zorder=2)
        ax.add_patch(card)

        ax.text(4.75, y + 0.2, title, fontsize=11.5, fontweight='bold', color=t_col, ha='center', va='center', zorder=3)
        ax.text(4.75, y - 0.22, desc, fontsize=9.0, color='#334155', ha='center', va='center', zorder=3)

    for i in range(len(stages) - 1):
        y_start = stages[i][5] - 0.48
        y_end = stages[i+1][5] + 0.56
        arrow = FancyArrowPatch((4.75, y_start), (4.75, y_end),
                                arrowstyle="Simple,tail_width=2.5,head_width=7.5,head_length=7.5",
                                color="#64748b", zorder=4)
        ax.add_patch(arrow)

    ax.text(4.75, 0.45, "Fig. 2. Stage-by-stage PySpark MLlib NLP feature transformation and sentiment inference pipeline.",
            fontsize=9.0, fontstyle='italic', color='#64748b', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/spark_nlp_pipeline.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated clean spark_nlp_pipeline.png")


# ==============================================================================
# 3. Dual Storage Tiering (Neat & Clean)
# ==============================================================================
def draw_neat_storage_tiering():
    fig, ax = plt.subplots(figsize=(9.5, 11.0), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 9.5)
    ax.set_ylim(0, 11.0)
    ax.axis('off')

    ax.text(4.75, 10.4, "Dual Storage Architecture: Hot Store vs. Cold Lakehouse",
            fontsize=14.5, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.75, 10.0, "Operational Real-Time Serving vs. Compressed Columnar Analytics",
            fontsize=9.5, color='#475569', ha='center', va='center')

    # Producer Box
    card_in = FancyBboxPatch((1.25, 8.2), 7.0, 1.15, boxstyle="round,pad=0.08,rounding_size=0.12",
                             facecolor='#fff7ed', edgecolor='#ea580c', linewidth=2.0, zorder=2)
    ax.add_patch(card_in)
    ax.text(4.75, 8.95, "PySpark Structured Streaming Ingestion", fontsize=11.5, fontweight='bold', color='#c2410c', ha='center')
    ax.text(4.75, 8.5, "Micro-Batch Processing  •  Concurrent Dual-Sink Persistence", fontsize=9.0, color='#334155', ha='center')

    # Branch Arrows
    ax.add_patch(FancyArrowPatch((3.2, 8.2), (2.6, 7.0), arrowstyle="Simple,tail_width=2.2,head_width=7,head_length=7", color="#16a34a", zorder=3))
    ax.add_patch(FancyArrowPatch((6.3, 8.2), (6.9, 7.0), arrowstyle="Simple,tail_width=2.2,head_width=7,head_length=7", color="#0891b2", zorder=3))
    ax.text(2.4, 7.6, "Hot JSON Path", fontsize=8.5, fontweight='bold', color='#16a34a', ha='center')
    ax.text(7.1, 7.6, "Cold Snappy Path", fontsize=8.5, fontweight='bold', color='#0891b2', ha='center')

    # Hot Tier Box
    card_hot = FancyBboxPatch((0.6, 4.8), 3.9, 2.1, boxstyle="round,pad=0.08,rounding_size=0.12",
                              facecolor='#f0fdf4', edgecolor='#16a34a', linewidth=2.0, zorder=2)
    ax.add_patch(card_hot)
    ax.text(2.55, 6.5, "Hot Tier: MongoDB NoSQL", fontsize=11.5, fontweight='bold', color='#15803d', ha='center')
    ax.text(2.55, 5.55, "• Format: JSON Documents\n• Write Speed: High Throughput\n• Query Latency: < 50ms\n• Purpose: Real-Time UI Feed",
            fontsize=8.5, color='#334155', ha='center')

    # Cold Tier Box
    card_cold = FancyBboxPatch((5.0, 4.8), 3.9, 2.1, boxstyle="round,pad=0.08,rounding_size=0.12",
                               facecolor='#f0f9ff', edgecolor='#0891b2', linewidth=2.0, zorder=2)
    ax.add_patch(card_cold)
    ax.text(6.95, 6.5, "Cold Tier: Parquet Lakehouse", fontsize=11.5, fontweight='bold', color='#0e7490', ha='center')
    ax.text(6.95, 5.55, "• Format: Apache Parquet\n• Compression: ~82% Disk Saved\n• Query: Column Pruning & Scans\n• Purpose: Model Retraining",
            fontsize=8.5, color='#334155', ha='center')

    # Arrows to destinations
    ax.add_patch(FancyArrowPatch((2.55, 4.8), (2.55, 3.5), arrowstyle="Simple,tail_width=2.2,head_width=7,head_length=7", color="#16a34a", zorder=3))
    ax.add_patch(FancyArrowPatch((6.95, 4.8), (6.95, 3.5), arrowstyle="Simple,tail_width=2.2,head_width=7,head_length=7", color="#0891b2", zorder=3))

    # Serving Destinations
    card_ui = FancyBboxPatch((0.6, 1.9), 3.9, 1.5, boxstyle="round,pad=0.08,rounding_size=0.12",
                             facecolor='#fefce8', edgecolor='#ca8a04', linewidth=2.0, zorder=2)
    ax.add_patch(card_ui)
    ax.text(2.55, 2.95, "Flask Web Dashboard", fontsize=11.0, fontweight='bold', color='#a16207', ha='center')
    ax.text(2.55, 2.35, "Live sentiment distribution charts,\nKPI meters, & tweet verification", fontsize=8.5, color='#334155', ha='center')

    card_analytics = FancyBboxPatch((5.0, 1.9), 3.9, 1.5, boxstyle="round,pad=0.08,rounding_size=0.12",
                                    facecolor='#fdf2f8', edgecolor='#db2777', linewidth=2.0, zorder=2)
    ax.add_patch(card_analytics)
    ax.text(6.95, 2.95, "PySpark Retraining Engine", fontsize=11.0, fontweight='bold', color='#be185d', ha='center')
    ax.text(6.95, 2.35, "Batch dataset consolidation,\nfeature refitting, & evaluations", fontsize=8.5, color='#334155', ha='center')

    # Caption
    ax.text(4.75, 0.7, "Fig. 3. Architectural comparison between Hot Operational Store (MongoDB) and Cold Analytical Lakehouse (Parquet).",
            fontsize=9.0, fontstyle='italic', color='#64748b', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/lakehouse_tiering.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated clean lakehouse_tiering.png")


# ==============================================================================
# 4. Human-in-the-Loop Active Learning (Neat & Clean)
# ==============================================================================
def draw_neat_active_learning():
    fig, ax = plt.subplots(figsize=(9.5, 12.5), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 9.5)
    ax.set_ylim(0, 12.5)
    ax.axis('off')

    ax.text(4.75, 11.8, "Human-in-the-Loop Active Learning Loop",
            fontsize=15.5, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(4.75, 11.4, "Continuous Model Evolution via Real-Time Human Feedback & PySpark Retraining",
            fontsize=10.0, color='#475569', ha='center', va='center')

    steps = [
        ("Step 1: Real-Time Tweet Feed Inspection", "Human operator inspects incoming live tweet stream and model predictions on web UI", "#f0f9ff", "#0284c7", "#0369a1", 10.0),
        ("Step 2: Interactive Label Correction", "Operator clicks correction badge (+Positive, -Negative, Neutral, Irrelevant) on misclassified tweets", "#fdf2f8", "#db2777", "#be185d", 8.1),
        ("Step 3: Ground-Truth MongoDB Persistence", "MongoDB updates document status to 'is_verified: true' with the verified sentiment label", "#f0fdf4", "#16a34a", "#15803d", 6.2),
        ("Step 4: Automated PySpark Retraining", "spark_retrainer.py consolidates verified records with baseline dataset and refits MLlib pipeline", "#fff7ed", "#ea580c", "#c2410c", 4.3),
        ("Step 5: Dynamic Artifact Hot-Reload", "Updated PipelineModel is exported to disk and dynamically reloaded into the streaming consumer", "#f5f3ff", "#7c3aed", "#6d28d9", 2.4)
    ]

    for title, desc, bg, border, t_col, y in steps:
        shadow = FancyBboxPatch((1.05, y - 0.54), 7.4, 1.08, boxstyle="round,pad=0.08,rounding_size=0.12",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        card = FancyBboxPatch((1.0, y - 0.5), 7.5, 1.08, boxstyle="round,pad=0.08,rounding_size=0.12",
                              facecolor=bg, edgecolor=border, linewidth=2.0, zorder=2)
        ax.add_patch(card)

        ax.text(4.75, y + 0.2, title, fontsize=11.5, fontweight='bold', color=t_col, ha='center', va='center', zorder=3)
        ax.text(4.75, y - 0.22, desc, fontsize=9.0, color='#334155', ha='center', va='center', zorder=3)

    for i in range(len(steps) - 1):
        y_start = steps[i][5] - 0.5
        y_end = steps[i+1][5] + 0.58
        arrow = FancyArrowPatch((4.75, y_start), (4.75, y_end),
                                arrowstyle="Simple,tail_width=2.5,head_width=7.5,head_length=7.5",
                                color="#64748b", zorder=4)
        ax.add_patch(arrow)

    # Active Feedback Loop line back to top
    ax.add_patch(FancyArrowPatch((8.5, 2.4), (8.9, 2.4), arrowstyle="-", color="#7c3aed", linewidth=2.0, zorder=4))
    ax.add_patch(FancyArrowPatch((8.9, 2.4), (8.9, 10.0), arrowstyle="-", color="#7c3aed", linewidth=2.0, zorder=4))
    ax.add_patch(FancyArrowPatch((8.9, 10.0), (8.5, 10.0), arrowstyle="Simple,tail_width=2.5,head_width=7.5,head_length=7.5", color="#7c3aed", zorder=4))
    ax.text(8.9, 6.2, "Continuous Retraining Feedback Loop", fontsize=8.5, fontweight='bold', color='#7c3aed',
            ha='center', va='center', rotation=90, bbox=dict(boxstyle="round,pad=0.25", facecolor="#ffffff", edgecolor="#7c3aed", linewidth=0.8, zorder=5))

    # Caption
    ax.text(4.75, 0.7, "Fig. 4. Closed-loop active learning architecture showing human feedback, ground truth storage, and model retraining.",
            fontsize=9.0, fontstyle='italic', color='#64748b', ha='center', va='center')

    plt.tight_layout()
    plt.savefig("project_diagrams/active_learning_flow.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Generated clean active_learning_flow.png")


if __name__ == "__main__":
    draw_neat_architecture()
    draw_neat_nlp_pipeline()
    draw_neat_storage_tiering()
    draw_neat_active_learning()
    print("All readable, neat project diagrams generated successfully!")
