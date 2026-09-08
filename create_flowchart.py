import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Polygon

os.makedirs("project_diagrams", exist_ok=True)

plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['font.family'] = 'sans-serif'

def generate_clean_flowchart():
    fig, ax = plt.subplots(figsize=(18, 10), dpi=300)
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Header
    ax.text(9, 9.4, "Real-Time Twitter Sentiment Analysis - Pipeline Flowchart",
            fontsize=20, fontweight='bold', color='#0f172a', ha='center', va='center')
    ax.text(9, 9.0, "End-to-End Data Ingestion, PySpark Stream Processing, ML Classification & Persistence Flow",
            fontsize=11.5, color='#64748b', ha='center', va='center')

    # Helper: Draw Process Block (Rectangle)
    def draw_process(x, y, w, h, title, subtitle="", bg="#f8fafc", border="#3b82f6"):
        # Shadow
        shadow = FancyBboxPatch((x+0.04, y-0.04), w, h, boxstyle="round,pad=0.08,rounding_size=0.15",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)
        # Box
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08,rounding_size=0.15",
                             facecolor=bg, edgecolor=border, linewidth=2, zorder=2)
        ax.add_patch(box)
        if subtitle:
            ax.text(x + w/2, y + h/2 + 0.22, title, fontsize=10.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=3)
            ax.text(x + w/2, y + h/2 - 0.25, subtitle, fontsize=8.5, color='#475569', ha='center', va='center', zorder=3)
        else:
            ax.text(x + w/2, y + h/2, title, fontsize=10.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=3)

    # Helper: Draw Diamond (Decision)
    def draw_diamond(cx, cy, r_w, r_h, title, subtitle="", bg="#fff7ed", border="#ea580c"):
        # Diamond vertices
        pts = [[cx, cy + r_h], [cx + r_w, cy], [cx, cy - r_h], [cx - r_w, cy]]
        poly_shadow = Polygon([[p[0]+0.04, p[1]-0.04] for p in pts], closed=True, facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(poly_shadow)
        poly = Polygon(pts, closed=True, facecolor=bg, edgecolor=border, linewidth=2, zorder=2)
        ax.add_patch(poly)
        if subtitle:
            ax.text(cx, cy + 0.15, title, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=3)
            ax.text(cx, cy - 0.2, subtitle, fontsize=7.5, color='#475569', ha='center', va='center', zorder=3)
        else:
            ax.text(cx, cy, title, fontsize=9.5, fontweight='bold', color='#0f172a', ha='center', va='center', zorder=3)

    # Helper: Draw Storage Cylinder / Database
    def draw_db(x, y, w, h, title, subtitle="", bg="#f0fdf4", border="#16a34a"):
        shadow = FancyBboxPatch((x+0.04, y-0.04), w, h, boxstyle="round,pad=0.08,rounding_size=0.25",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08,rounding_size=0.25",
                             facecolor=bg, edgecolor=border, linewidth=2, zorder=2)
        ax.add_patch(box)
        ax.text(x + w/2, y + h/2 + 0.2, title, fontsize=10.5, fontweight='bold', color='#16a34a', ha='center', va='center', zorder=3)
        if subtitle:
            ax.text(x + w/2, y + h/2 - 0.25, subtitle, fontsize=8.5, color='#334155', ha='center', va='center', zorder=3)

    # Helper: Arrow
    def draw_arrow(p1, p2, label="", col="#475569", rad=0.0, label_pos=(0, 0.2)):
        kw = dict(arrowstyle="Simple,tail_width=2,head_width=7,head_length=7", color=col, zorder=4)
        if rad != 0.0:
            arrow = FancyArrowPatch(p1, p2, connectionstyle=f"arc3,rad={rad}", **kw)
        else:
            arrow = FancyArrowPatch(p1, p2, **kw)
        ax.add_patch(arrow)
        if label:
            mx = (p1[0] + p2[0]) / 2 + label_pos[0]
            my = (p1[1] + p2[1]) / 2 + label_pos[1]
            ax.text(mx, my, label, fontsize=8, fontweight='bold', color=col, ha='center', va='center',
                    bbox=dict(boxstyle="round,pad=0.18", facecolor="#ffffff", edgecolor=col, linewidth=0.8, zorder=5))

    # ================= TOP STREAMING ROW (Steps 1 to 5) =================
    # 1. Start / Twitter Ingestion
    draw_process(0.6, 6.4, 2.8, 1.6, "1. Twitter Ingestion", "Live Scraper / CSV Stream", "#f0f9ff", "#0284c7")

    # 2. Kafka Producer
    draw_process(4.1, 6.4, 2.8, 1.6, "2. Kafka Producer", "kafka_producer.py", "#f0f9ff", "#0284c7")

    # 3. Kafka Topic
    draw_process(7.6, 6.4, 2.8, 1.6, "3. Kafka Broker", "Topic: 'numtest'", "#f8fafc", "#475569")

    # 4. PySpark Streaming Consumer
    draw_process(11.1, 6.4, 3.0, 1.6, "4. PySpark Consumer", "spark_consumer.py", "#fff7ed", "#ea580c")

    # 5. NLP Feature Extraction
    draw_process(14.8, 6.4, 2.6, 1.6, "5. NLP Pipeline", "Regex, Tokenizer, TF-IDF", "#fff7ed", "#ea580c")

    # Top Row Arrows
    draw_arrow((3.4, 7.2), (4.1, 7.2), "Fetch Tweets", "#0284c7")
    draw_arrow((6.9, 7.2), (7.6, 7.2), "Publish Message", "#0284c7")
    draw_arrow((10.4, 7.2), (11.1, 7.2), "Stream Feed", "#475569")
    draw_arrow((14.1, 7.2), (14.8, 7.2), "Transform", "#ea580c")

    # Turn down to Middle Row
    draw_arrow((16.1, 6.4), (16.1, 5.2), "", "#ea580c")

    # ================= MIDDLE ROW (Steps 6 to 9) =================
    # 6. MLlib Classifier
    draw_process(14.4, 3.6, 3.0, 1.6, "6. PySpark MLlib", "Logistic Regression Model", "#f5f3ff", "#7c3aed")

    # 7. Sentiment Classification Decision
    draw_diamond(12.0, 4.4, 1.4, 1.0, "7. Sentiment?", "Pos / Neg / Neu / Irr", "#fdf2f8", "#db2777")

    # 8. Persistence (MongoDB + Parquet)
    draw_db(6.8, 3.6, 3.2, 1.6, "8. Storage Tiering", "MongoDB (Hot) & Parquet (Cold)", "#f0fdf4", "#16a34a")

    # 9. Serving Web Dashboard
    draw_process(1.5, 3.6, 3.6, 1.6, "9. Web Dashboard", "Flask UI (Port 8000) & Charts", "#fefce8", "#ca8a04")

    # Middle Row Arrows
    draw_arrow((16.1, 5.2), (15.9, 5.2), "", "#7c3aed")
    draw_arrow((14.4, 4.4), (13.4, 4.4), "Predict", "#7c3aed")
    draw_arrow((10.6, 4.4), (10.0, 4.4), "Classified Result", "#db2777")
    draw_arrow((6.8, 4.4), (5.1, 4.4), "Live Query Feed", "#16a34a")

    # ================= BOTTOM ROW (Feedback & Active Learning) =================
    # 10. Human Feedback
    draw_process(1.5, 0.8, 3.6, 1.6, "10. User Feedback", "Verify (+Pos, -Neg, Neu, Irr)", "#fdf2f8", "#db2777")

    # 11. MongoDB Ground-Truth Update
    draw_db(6.8, 0.8, 3.2, 1.6, "11. Ground Truth Sync", "MongoDB (is_verified: true)", "#f0fdf4", "#16a34a")

    # 12. Automated Batch Retraining
    draw_process(11.5, 0.8, 3.8, 1.6, "12. PySpark Retrainer", "spark_retrainer.py (Fit & Overwrite)", "#fff7ed", "#ea580c")

    # Bottom Row Arrows
    draw_arrow((3.3, 3.6), (3.3, 2.4), "User Inspects & Corrects", "#db2777")
    draw_arrow((5.1, 1.6), (6.8, 1.6), "POST /api/verify-sentiment", "#db2777")
    draw_arrow((10.0, 1.6), (11.5, 1.6), "Fetch Verified Records", "#16a34a")
    draw_arrow((15.3, 1.6), (15.9, 3.6), "Reload Model Artifact", "#ea580c", rad=0.25)

    plt.tight_layout()
    plt.savefig("project_diagrams/architecture_diagram.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.savefig("project_diagrams/system_flowchart.png", dpi=300, bbox_inches='tight', facecolor='#ffffff')
    plt.close()
    print("Flowchart generated successfully as architecture_diagram.png and system_flowchart.png!")

if __name__ == "__main__":
    generate_clean_flowchart()
