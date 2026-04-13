#!/usr/bin/env python3
"""Generate PDF report: Cloud Run vs Kubernetes for DynaStore/GeoID Platform."""

import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm, mm
from reportlab.lib.colors import (
    HexColor, white, black, Color, transparent,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle,
    KeepTogether, Frame, PageTemplate, BaseDocTemplate, NextPageTemplate,
    Flowable,
)
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Circle, Polygon, Group
from reportlab.graphics.charts.barcharts import VerticalBarChart, HorizontalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.charts.legends import Legend
from reportlab.graphics import renderPDF
from reportlab.graphics.widgets.markers import makeMarker

# ── Colour palette ──────────────────────────────────────────────────────────
BLUE_PRIMARY   = HexColor("#1a73e8")
BLUE_DARK      = HexColor("#0d47a1")
BLUE_LIGHT     = HexColor("#e8f0fe")
GREEN_OK       = HexColor("#34a853")
GREEN_LIGHT    = HexColor("#e6f4ea")
ORANGE_WARN    = HexColor("#f9ab00")
ORANGE_LIGHT   = HexColor("#fef7e0")
RED_ALERT      = HexColor("#ea4335")
GREY_TEXT      = HexColor("#5f6368")
GREY_LIGHT     = HexColor("#f1f3f4")
GREY_BORDER    = HexColor("#dadce0")
GREY_DARK      = HexColor("#3c4043")
WHITE          = white
K8S_BLUE       = HexColor("#326ce5")
DOCKER_BLUE    = HexColor("#2496ed")
CLOUD_RUN_BLUE = HexColor("#4285f4")

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm


# ── Helper flowables ────────────────────────────────────────────────────────
class HRule(Flowable):
    """Horizontal rule."""
    def __init__(self, width, color=GREY_BORDER, thickness=0.5):
        super().__init__()
        self.line_width = width
        self.color = color
        self.thickness = thickness

    def wrap(self, availWidth, availHeight):
        self.line_width = min(self.line_width, availWidth)
        return (self.line_width, self.thickness + 4)

    def draw(self):
        self.canv.setStrokeColor(self.color)
        self.canv.setLineWidth(self.thickness)
        self.canv.line(0, 2, self.line_width, 2)


class ColorBlock(Flowable):
    """Full-width coloured block behind content."""
    def __init__(self, content, bg_color=BLUE_LIGHT, padding=12, border_color=None):
        super().__init__()
        self.content = content
        self.bg = bg_color
        self.padding = padding
        self.border_color = border_color

    def wrap(self, availWidth, availHeight):
        w, h = self.content.wrap(availWidth - 2 * self.padding, availHeight)
        self.content_w = w
        self.content_h = h
        return (availWidth, h + 2 * self.padding)

    def draw(self):
        p = self.padding
        w = self.content_w + 2 * p
        h = self.content_h + 2 * p
        self.canv.setFillColor(self.bg)
        if self.border_color:
            self.canv.setStrokeColor(self.border_color)
            self.canv.setLineWidth(1)
            self.canv.roundRect(0, 0, w, h, 4, fill=1, stroke=1)
        else:
            self.canv.roundRect(0, 0, w, h, 4, fill=1, stroke=0)
        self.content.drawOn(self.canv, p, p)


# ── Styles ──────────────────────────────────────────────────────────────────
def build_styles():
    ss = getSampleStyleSheet()
    ss.add(ParagraphStyle(
        "CoverTitle", parent=ss["Title"],
        fontSize=32, leading=38, textColor=WHITE,
        alignment=TA_LEFT, spaceAfter=12,
        fontName="Helvetica-Bold",
    ))
    ss.add(ParagraphStyle(
        "CoverSub", parent=ss["Normal"],
        fontSize=16, leading=20, textColor=HexColor("#b3d4ff"),
        alignment=TA_LEFT, fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "SectionTitle", parent=ss["Heading1"],
        fontSize=22, leading=26, textColor=BLUE_DARK,
        spaceBefore=24, spaceAfter=10,
        fontName="Helvetica-Bold",
    ))
    ss.add(ParagraphStyle(
        "SubSection", parent=ss["Heading2"],
        fontSize=15, leading=19, textColor=GREY_DARK,
        spaceBefore=16, spaceAfter=8,
        fontName="Helvetica-Bold",
    ))
    ss.add(ParagraphStyle(
        "Body", parent=ss["Normal"],
        fontSize=10, leading=14, textColor=GREY_DARK,
        alignment=TA_JUSTIFY, spaceAfter=8,
        fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "BodyBold", parent=ss["Normal"],
        fontSize=10, leading=14, textColor=GREY_DARK,
        alignment=TA_JUSTIFY, spaceAfter=8,
        fontName="Helvetica-Bold",
    ))
    ss.add(ParagraphStyle(
        "BulletCustom", parent=ss["Normal"],
        fontSize=10, leading=14, textColor=GREY_DARK,
        leftIndent=18, bulletIndent=6, spaceAfter=4,
        fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "Caption", parent=ss["Normal"],
        fontSize=9, leading=12, textColor=GREY_TEXT,
        alignment=TA_CENTER, spaceBefore=4, spaceAfter=12,
        fontName="Helvetica-Oblique",
    ))
    ss.add(ParagraphStyle(
        "Callout", parent=ss["Normal"],
        fontSize=10, leading=14, textColor=BLUE_DARK,
        alignment=TA_LEFT, spaceAfter=4,
        fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "Footer", parent=ss["Normal"],
        fontSize=8, leading=10, textColor=GREY_TEXT,
        alignment=TA_CENTER,
        fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "TableHeader", parent=ss["Normal"],
        fontSize=9, leading=12, textColor=WHITE,
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
    ))
    ss.add(ParagraphStyle(
        "TableCell", parent=ss["Normal"],
        fontSize=9, leading=12, textColor=GREY_DARK,
        alignment=TA_CENTER,
        fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "TableCellLeft", parent=ss["Normal"],
        fontSize=9, leading=12, textColor=GREY_DARK,
        alignment=TA_LEFT,
        fontName="Helvetica",
    ))
    ss.add(ParagraphStyle(
        "Verdict", parent=ss["Normal"],
        fontSize=11, leading=15, textColor=GREEN_OK,
        alignment=TA_LEFT, spaceAfter=8,
        fontName="Helvetica-Bold",
    ))
    return ss


# ── Chart builders ──────────────────────────────────────────────────────────

def make_scorecard_bar_chart():
    """Grouped bar chart: Cloud Run vs K8s vs On-Prem Docker across dimensions."""
    d = Drawing(480, 260)

    dimensions = [
        "Ops\nSimplicity", "Scaling", "Cost\n(Low)", "Cost\n(High)",
        "Cold\nStart", "Heavy\nWorkloads", "Observ-\nability",
        "Security", "Dev\nExperience", "Porta-\nbility",
    ]
    cloud_run = [9, 8, 10, 5, 5, 5, 9, 9, 8, 6]
    k8s       = [4, 10, 3, 8, 9, 9, 7, 7, 5, 9]
    docker    = [6, 2, 7, 9, 10, 8, 3, 4, 9, 7]

    chart = VerticalBarChart()
    chart.x = 50
    chart.y = 45
    chart.width = 400
    chart.height = 175
    chart.data = [cloud_run, k8s, docker]
    chart.categoryAxis.categoryNames = dimensions
    chart.categoryAxis.labels.fontSize = 7
    chart.categoryAxis.labels.textAnchor = "middle"
    chart.categoryAxis.labels.dy = -5
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = 10
    chart.valueAxis.valueStep = 2
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.gridStrokeDashArray = (2, 2)
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = GREY_BORDER
    chart.barWidth = 8
    chart.groupSpacing = 12
    chart.barSpacing = 2

    chart.bars[0].fillColor = CLOUD_RUN_BLUE
    chart.bars[1].fillColor = K8S_BLUE
    chart.bars[2].fillColor = DOCKER_BLUE

    d.add(chart)

    legend = Legend()
    legend.x = 140
    legend.y = 245
    legend.dx = 8
    legend.dy = 8
    legend.deltax = 80
    legend.fontSize = 9
    legend.alignment = "right"
    legend.colorNamePairs = [
        (CLOUD_RUN_BLUE, "Cloud Run"),
        (K8S_BLUE, "Kubernetes"),
        (DOCKER_BLUE, "On-Prem Docker"),
    ]
    d.add(legend)

    return d


def make_total_score_pie():
    """Pie chart showing total scores."""
    d = Drawing(240, 180)
    pie = Pie()
    pie.x = 40
    pie.y = 15
    pie.width = 140
    pie.height = 140
    pie.data = [74, 71, 65]
    pie.labels = ["Cloud Run\n74/100", "Kubernetes\n71/100", "Docker\n65/100"]
    pie.slices[0].fillColor = CLOUD_RUN_BLUE
    pie.slices[1].fillColor = K8S_BLUE
    pie.slices[2].fillColor = DOCKER_BLUE
    pie.slices[0].popout = 6
    pie.slices.strokeColor = WHITE
    pie.slices.strokeWidth = 2
    pie.slices.fontSize = 8
    pie.slices.fontName = "Helvetica-Bold"
    pie.slices[0].fontColor = CLOUD_RUN_BLUE
    pie.slices[1].fontColor = K8S_BLUE
    pie.slices[2].fontColor = DOCKER_BLUE
    pie.sideLabels = True
    pie.sideLabelsOffset = 0.08
    d.add(pie)
    return d


def make_ops_burden_chart():
    """Horizontal bar chart: operational burden hours/month estimate."""
    d = Drawing(440, 140)

    chart = HorizontalBarChart()
    chart.x = 120
    chart.y = 15
    chart.width = 290
    chart.height = 105
    chart.data = [[5, 40, 25]]
    chart.categoryAxis.categoryNames = ["On-Prem Docker", "Kubernetes", "Cloud Run"]
    chart.categoryAxis.labels.fontSize = 9
    chart.categoryAxis.labels.dx = -5
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = 50
    chart.valueAxis.valueStep = 10
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = GREY_BORDER
    chart.valueAxis.gridStrokeDashArray = (2, 2)
    chart.barWidth = 18

    chart.bars[0].fillColor = BLUE_PRIMARY

    # Value labels
    d.add(chart)
    d.add(String(120 + 290 * (5/50) + 5, 98, "~5 hrs", fontSize=8, fillColor=GREY_TEXT))
    d.add(String(120 + 290 * (40/50) + 5, 58, "~40 hrs", fontSize=8, fillColor=RED_ALERT))
    d.add(String(120 + 290 * (25/50) + 5, 18, "~25 hrs", fontSize=8, fillColor=ORANGE_WARN))

    return d


def make_cost_curve_chart():
    """Line chart: monthly cost vs requests/month for each platform."""
    d = Drawing(460, 220)

    # X = thousands of requests/day, Y = estimated monthly cost USD
    # Cloud Run: low base, linear scale
    cr_data = [(0, 0), (1, 30), (5, 80), (10, 150), (20, 300), (50, 750), (100, 1500)]
    # K8s: high base, slower growth
    k8s_data = [(0, 350), (1, 360), (5, 380), (10, 420), (20, 500), (50, 700), (100, 1100)]
    # On-prem: flat (hardware already paid)
    docker_data = [(0, 200), (1, 200), (5, 200), (10, 210), (20, 220), (50, 250), (100, 300)]

    chart = LinePlot()
    chart.x = 60
    chart.y = 40
    chart.width = 370
    chart.height = 150
    chart.data = [cr_data, k8s_data, docker_data]

    chart.xValueAxis.valueMin = 0
    chart.xValueAxis.valueMax = 100
    chart.xValueAxis.valueStep = 20
    chart.xValueAxis.labels.fontSize = 8
    chart.xValueAxis.visibleGrid = True
    chart.xValueAxis.gridStrokeColor = GREY_BORDER
    chart.xValueAxis.gridStrokeDashArray = (2, 2)

    chart.yValueAxis.valueMin = 0
    chart.yValueAxis.valueMax = 1600
    chart.yValueAxis.valueStep = 200
    chart.yValueAxis.labels.fontSize = 8
    chart.yValueAxis.visibleGrid = True
    chart.yValueAxis.gridStrokeColor = GREY_BORDER
    chart.yValueAxis.gridStrokeDashArray = (2, 2)

    chart.lines[0].strokeColor = CLOUD_RUN_BLUE
    chart.lines[0].strokeWidth = 2.5
    chart.lines[1].strokeColor = K8S_BLUE
    chart.lines[1].strokeWidth = 2.5
    chart.lines[1].strokeDashArray = (6, 3)
    chart.lines[2].strokeColor = DOCKER_BLUE
    chart.lines[2].strokeWidth = 2.5
    chart.lines[2].strokeDashArray = (2, 2)

    chart.lines[0].symbol = makeMarker("Circle")
    chart.lines[0].symbol.fillColor = CLOUD_RUN_BLUE
    chart.lines[0].symbol.size = 4
    chart.lines[1].symbol = makeMarker("Square")
    chart.lines[1].symbol.fillColor = K8S_BLUE
    chart.lines[1].symbol.size = 4
    chart.lines[2].symbol = makeMarker("Diamond")
    chart.lines[2].symbol.fillColor = DOCKER_BLUE
    chart.lines[2].symbol.size = 4

    d.add(chart)

    # Axis labels
    d.add(String(230, 8, "Requests/day (thousands)", fontSize=9, fillColor=GREY_TEXT, textAnchor="middle"))
    d.add(String(14, 130, "Monthly", fontSize=9, fillColor=GREY_TEXT, textAnchor="middle"))
    d.add(String(14, 118, "Cost ($)", fontSize=9, fillColor=GREY_TEXT, textAnchor="middle"))

    # Crossover annotation
    d.add(String(305, 185, "Crossover zone", fontSize=8, fillColor=RED_ALERT, textAnchor="middle"))

    # Legend
    legend = Legend()
    legend.x = 130
    legend.y = 210
    legend.dx = 8
    legend.dy = 8
    legend.deltax = 95
    legend.fontSize = 9
    legend.colorNamePairs = [
        (CLOUD_RUN_BLUE, "Cloud Run"),
        (K8S_BLUE, "Kubernetes"),
        (DOCKER_BLUE, "On-Prem Docker"),
    ]
    d.add(legend)

    return d


def make_cold_start_chart():
    """Bar chart: cold start latency comparison."""
    d = Drawing(400, 150)

    chart = HorizontalBarChart()
    chart.x = 120
    chart.y = 15
    chart.width = 250
    chart.height = 105
    chart.data = [[0.1, 0.5, 10]]
    chart.categoryAxis.categoryNames = ["On-Prem Docker", "Kubernetes", "Cloud Run"]
    chart.categoryAxis.labels.fontSize = 9
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = 15
    chart.valueAxis.valueStep = 3
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = GREY_BORDER
    chart.valueAxis.gridStrokeDashArray = (2, 2)
    chart.barWidth = 18
    chart.bars[0].fillColor = ORANGE_WARN

    d.add(chart)
    d.add(String(120 + 250 * (0.1/15) + 4, 98, "~0.1s", fontSize=8, fillColor=GREEN_OK))
    d.add(String(120 + 250 * (0.5/15) + 4, 58, "~0.5s", fontSize=8, fillColor=GREEN_OK))
    d.add(String(120 + 250 * (10/15) + 4, 18, "~5-15s", fontSize=8, fillColor=RED_ALERT))

    return d


def make_team_size_chart():
    """Chart showing recommended platform by team size."""
    d = Drawing(460, 130)

    # Background zones
    d.add(Rect(50, 20, 130, 80, fillColor=HexColor("#e8f0fe"), strokeColor=None))
    d.add(Rect(180, 20, 140, 80, fillColor=HexColor("#fef7e0"), strokeColor=None))
    d.add(Rect(320, 20, 120, 80, fillColor=HexColor("#e6f4ea"), strokeColor=None))

    d.add(String(115, 85, "Cloud Run", fontSize=11, fillColor=CLOUD_RUN_BLUE, textAnchor="middle", fontName="Helvetica-Bold"))
    d.add(String(115, 70, "Recommended", fontSize=9, fillColor=CLOUD_RUN_BLUE, textAnchor="middle"))
    d.add(String(250, 85, "Either viable", fontSize=11, fillColor=ORANGE_WARN, textAnchor="middle", fontName="Helvetica-Bold"))
    d.add(String(250, 70, "(depends on needs)", fontSize=9, fillColor=ORANGE_WARN, textAnchor="middle"))
    d.add(String(380, 85, "Kubernetes", fontSize=11, fillColor=GREEN_OK, textAnchor="middle", fontName="Helvetica-Bold"))
    d.add(String(380, 70, "Justified", fontSize=9, fillColor=GREEN_OK, textAnchor="middle"))

    # Team size axis
    d.add(Line(50, 20, 440, 20, strokeColor=GREY_DARK, strokeWidth=1.5))
    for x, label in [(50, "1"), (115, "3"), (180, "5"), (250, "8"), (320, "10"), (380, "15"), (440, "20+")]:
        d.add(Line(x, 17, x, 23, strokeColor=GREY_DARK, strokeWidth=1))
        d.add(String(x, 5, label, fontSize=8, fillColor=GREY_TEXT, textAnchor="middle"))

    d.add(String(245, 118, "Engineering Team Size (people)", fontSize=10, fillColor=GREY_DARK, textAnchor="middle", fontName="Helvetica-Bold"))

    return d


def make_decision_matrix_chart():
    """Visual decision matrix with weighted scores for DynaStore context."""
    d = Drawing(460, 200)

    categories = ["Team < 5", "Variable\nTraffic", "GCP\nCompliance", "Zero\nMaintenance", "Heavy\nGDAL Tasks", "GPU\nNeeded"]
    # 1 = favors Cloud Run, -1 = favors K8s, 0 = neutral
    weights   = [1, 1, 1, 1, -0.5, -1]
    colors    = [GREEN_OK, GREEN_OK, GREEN_OK, GREEN_OK, ORANGE_WARN, RED_ALERT]

    bar_w = 50
    gap = 18
    start_x = 50

    for i, (cat, w, c) in enumerate(zip(categories, weights, colors)):
        x = start_x + i * (bar_w + gap)
        # Bar (upward = Cloud Run, downward = K8s)
        bar_h = abs(w) * 60
        y0 = 100
        if w >= 0:
            d.add(Rect(x, y0, bar_w, bar_h, fillColor=c, strokeColor=None))
            d.add(String(x + bar_w/2, y0 + bar_h + 4, "CR", fontSize=8, fillColor=c, textAnchor="middle", fontName="Helvetica-Bold"))
        else:
            d.add(Rect(x, y0 - bar_h, bar_w, bar_h, fillColor=c, strokeColor=None))
            d.add(String(x + bar_w/2, y0 - bar_h - 12, "K8s", fontSize=8, fillColor=c, textAnchor="middle", fontName="Helvetica-Bold"))

        # Category label
        d.add(String(x + bar_w/2, 15, cat, fontSize=7, fillColor=GREY_DARK, textAnchor="middle"))

    # Center line
    d.add(Line(40, 100, 450, 100, strokeColor=GREY_DARK, strokeWidth=1, strokeDashArray=(3, 3)))
    d.add(String(35, 160, "Favors", fontSize=8, fillColor=CLOUD_RUN_BLUE, textAnchor="end"))
    d.add(String(35, 148, "Cloud Run", fontSize=8, fillColor=CLOUD_RUN_BLUE, textAnchor="end"))
    d.add(String(35, 70, "Favors", fontSize=8, fillColor=K8S_BLUE, textAnchor="end"))
    d.add(String(35, 58, "K8s", fontSize=8, fillColor=K8S_BLUE, textAnchor="end"))

    # Net result
    net = sum(weights)
    d.add(String(420, 185, f"Net: +{net:.1f} CR", fontSize=10, fillColor=GREEN_OK, textAnchor="middle", fontName="Helvetica-Bold"))

    return d


# ── Page templates ──────────────────────────────────────────────────────────

def cover_page(canvas, doc):
    """Draw the cover page background."""
    canvas.saveState()
    # Full-page gradient-like block
    canvas.setFillColor(BLUE_DARK)
    canvas.rect(0, PAGE_H * 0.35, PAGE_W, PAGE_H * 0.65, fill=1, stroke=0)
    # Accent bar
    canvas.setFillColor(CLOUD_RUN_BLUE)
    canvas.rect(0, PAGE_H * 0.35, PAGE_W, 4, fill=1, stroke=0)
    # Bottom section
    canvas.setFillColor(WHITE)
    canvas.rect(0, 0, PAGE_W, PAGE_H * 0.35, fill=1, stroke=0)
    # Footer
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(GREY_TEXT)
    canvas.drawCentredString(PAGE_W / 2, 20, "FAO - Agro-Informatics Platform | Confidential")
    canvas.restoreState()


def normal_page(canvas, doc):
    """Header/footer for content pages."""
    canvas.saveState()
    # Header bar
    canvas.setFillColor(BLUE_DARK)
    canvas.rect(0, PAGE_H - 28, PAGE_W, 28, fill=1, stroke=0)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.setFillColor(WHITE)
    canvas.drawString(MARGIN, PAGE_H - 19, "Infrastructure Decision Report")
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 19, "Cloud Run vs Kubernetes")
    # Footer
    canvas.setStrokeColor(GREY_BORDER)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, 35, PAGE_W - MARGIN, 35)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(GREY_TEXT)
    canvas.drawString(MARGIN, 22, "DynaStore / GeoID Platform")
    canvas.drawCentredString(PAGE_W / 2, 22, f"Page {doc.page}")
    canvas.drawRightString(PAGE_W - MARGIN, 22, "March 2026")
    canvas.restoreState()


# ── Content builders ────────────────────────────────────────────────────────

def build_cover(styles):
    """Cover page content."""
    story = []
    story.append(Spacer(1, PAGE_H * 0.18))
    story.append(Paragraph("Infrastructure<br/>Decision Report", styles["CoverTitle"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "Cloud Run vs Kubernetes vs On-Premise Docker<br/>"
        "for the DynaStore / GeoID Geospatial Platform",
        styles["CoverSub"],
    ))
    story.append(Spacer(1, 24))
    story.append(Paragraph(
        "FAO - Agro-Informatics Platform (AIP)<br/>March 2026",
        styles["CoverSub"],
    ))
    story.append(NextPageTemplate("content"))
    story.append(PageBreak())
    return story


def build_executive_summary(styles):
    story = []
    story.append(Paragraph("1. Executive Summary", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    verdict = Paragraph(
        "<b>Recommendation: Stay on Google Cloud Run</b> for the current and medium-term horizon. "
        "The platform's architecture is already runtime-agnostic; a future migration to Kubernetes "
        "is feasible but not justified today.",
        styles["Callout"],
    )
    story.append(ColorBlock(verdict, bg_color=GREEN_LIGHT, border_color=GREEN_OK))
    story.append(Spacer(1, 8))

    story.append(Paragraph(
        "This report evaluates three deployment strategies for the DynaStore/GeoID geospatial "
        "platform: <b>Google Cloud Run</b> (current), <b>Kubernetes</b> (GKE or self-hosted), and "
        "<b>on-premise Docker Compose</b>. The analysis considers operational complexity, cost "
        "structure, scaling behaviour, security posture, developer experience, and the specific "
        "constraints of the FAO/UN context (compliance, team size, geospatial workloads).",
        styles["Body"],
    ))

    story.append(Paragraph(
        "The evaluation yields a <b>74/100 score for Cloud Run</b> vs 71/100 for Kubernetes and "
        "65/100 for on-premise Docker. While the margin is narrow, Cloud Run wins decisively on "
        "the dimensions that matter most for a small team operating a geospatial platform: "
        "<b>operational simplicity, built-in observability, security compliance, and zero "
        "infrastructure maintenance</b>.",
        styles["Body"],
    ))

    key_findings = [
        ("Operational Cost:", "Cloud Run eliminates ~35 hours/month of infrastructure maintenance compared to self-managed Kubernetes."),
        ("Financial Cost:", "At current traffic levels (moderate, bursty), Cloud Run's pay-per-request model is more economical than a committed Kubernetes cluster."),
        ("Cold Start Tradeoff:", "The GDAL base image (~2-3 GB) causes 5-15s cold starts on Cloud Run. This is mitigated by min-instances=1, which still costs less than a K8s cluster."),
        ("Architecture Readiness:", "The protocol-based, plugin-driven architecture is fully runtime-agnostic. Migration to K8s would require only deployment config changes, not application code changes."),
        ("Future Trigger:", "Re-evaluate when 3+ of these become true: GPU needed, sustained high traffic, tasks exceeding 32 GiB, service mesh required, multi-cloud mandate."),
    ]
    for title, body in key_findings:
        story.append(Paragraph(f"\u2022 <b>{title}</b> {body}", styles["BulletCustom"]))

    story.append(PageBreak())
    return story


def build_architecture_overview(styles):
    story = []
    story.append(Paragraph("2. Current Architecture Overview", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    story.append(Paragraph(
        "DynaStore is a <b>multi-service Python/FastAPI platform</b> with geospatial capabilities "
        "(GDAL, PostGIS). It follows a three-pillar architecture: <b>Modules</b> (backend-agnostic "
        "domain libraries), <b>Extensions</b> (stateless API adapters), and <b>Tasks</b> "
        "(asynchronous background workers).",
        styles["Body"],
    ))

    # Architecture table
    data = [
        [Paragraph("<b>Component</b>", styles["TableHeader"]),
         Paragraph("<b>Current Runtime</b>", styles["TableHeader"]),
         Paragraph("<b>Role</b>", styles["TableHeader"]),
         Paragraph("<b>Cloud Run Fit</b>", styles["TableHeader"])],
        [Paragraph("API Service", styles["TableCellLeft"]),
         Paragraph("Cloud Run Service", styles["TableCell"]),
         Paragraph("REST API (4+ workers)", styles["TableCellLeft"]),
         Paragraph("Excellent", styles["TableCell"])],
        [Paragraph("GeoID Portal", styles["TableCellLeft"]),
         Paragraph("Cloud Run Service", styles["TableCell"]),
         Paragraph("Web portal UI", styles["TableCellLeft"]),
         Paragraph("Excellent", styles["TableCell"])],
        [Paragraph("Worker Service", styles["TableCellLeft"]),
         Paragraph("Cloud Run Service", styles["TableCell"]),
         Paragraph("Task dispatcher", styles["TableCellLeft"]),
         Paragraph("Good", styles["TableCell"])],
        [Paragraph("Task Jobs", styles["TableCellLeft"]),
         Paragraph("Cloud Run Jobs", styles["TableCell"]),
         Paragraph("One-shot processing", styles["TableCellLeft"]),
         Paragraph("Good*", styles["TableCell"])],
        [Paragraph("PostgreSQL", styles["TableCellLeft"]),
         Paragraph("Cloud SQL", styles["TableCell"]),
         Paragraph("Primary data store", styles["TableCellLeft"]),
         Paragraph("Excellent", styles["TableCell"])],
        [Paragraph("Elasticsearch", styles["TableCellLeft"]),
         Paragraph("Elastic Cloud", styles["TableCell"]),
         Paragraph("Full-text search", styles["TableCellLeft"]),
         Paragraph("Good", styles["TableCell"])],
    ]

    t = Table(data, colWidths=[100, 105, 130, 80])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("BACKGROUND", (0, 1), (-1, -1), WHITE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Paragraph(
        "* Cloud Run Jobs have a 8 vCPU / 32 GiB ceiling and memory-backed tmpfs. "
        "Heavy GDAL raster processing may approach these limits.",
        styles["Caption"],
    ))

    story.append(Paragraph("Key Architectural Strengths (Runtime-Agnostic)", styles["SubSection"]))
    strengths = [
        "Plugin-based module/extension/task discovery via PEP-517 entry points",
        "Protocol-based decoupling \u2014 no hard imports between modules",
        "PostgreSQL-native task queue (LISTEN/NOTIFY + SKIP LOCKED) \u2014 no Redis/RabbitMQ dependency",
        "Leader election via pg_try_advisory_lock (scales to 500+ instances with ~5 LISTEN connections)",
        "Durable outbox pattern for events (survives crashes, retry-enabled)",
        "SCOPE-based Docker builds enable per-service dependency trees from a single codebase",
    ]
    for s in strengths:
        story.append(Paragraph(f"\u2022 {s}", styles["BulletCustom"]))

    story.append(Spacer(1, 6))
    note = Paragraph(
        "These design choices mean the application <b>does not depend on Cloud Run features</b>. "
        "A migration to Kubernetes would affect only deployment configuration, not application code. "
        "This is a deliberate architectural decision that preserves optionality.",
        styles["Callout"],
    )
    story.append(ColorBlock(note, bg_color=BLUE_LIGHT, border_color=BLUE_PRIMARY))

    story.append(PageBreak())
    return story


def build_comparison_charts(styles):
    story = []
    story.append(Paragraph("3. Comparative Analysis", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    # ── Scorecard chart ──
    story.append(Paragraph("3.1 Overall Scorecard (0\u201310 per dimension)", styles["SubSection"]))
    story.append(make_scorecard_bar_chart())
    story.append(Paragraph(
        "Figure 1: Multi-dimensional comparison across 10 evaluation criteria. "
        "Cloud Run leads in 5 dimensions, Kubernetes in 4, On-Prem Docker in 1.",
        styles["Caption"],
    ))

    # ── Scorecard table ──
    score_data = [
        [Paragraph("<b>Dimension</b>", styles["TableHeader"]),
         Paragraph("<b>Cloud Run</b>", styles["TableHeader"]),
         Paragraph("<b>K8s</b>", styles["TableHeader"]),
         Paragraph("<b>Docker</b>", styles["TableHeader"]),
         Paragraph("<b>Winner</b>", styles["TableHeader"])],
        *[[Paragraph(d, styles["TableCellLeft"]),
           Paragraph(str(cr), styles["TableCell"]),
           Paragraph(str(k), styles["TableCell"]),
           Paragraph(str(dc), styles["TableCell"]),
           Paragraph(w, styles["TableCell"])]
          for d, cr, k, dc, w in [
              ("Ops Simplicity", 9, 4, 6, "Cloud Run"),
              ("Scaling", 8, 10, 2, "K8s"),
              ("Cost (Low Traffic)", 10, 3, 7, "Cloud Run"),
              ("Cost (High Traffic)", 5, 8, 9, "Docker"),
              ("Cold Start Perf", 5, 9, 10, "Docker"),
              ("Heavy Workloads", 5, 9, 8, "K8s"),
              ("Observability", 9, 7, 3, "Cloud Run"),
              ("Security/Compliance", 9, 7, 4, "Cloud Run"),
              ("Dev Experience", 8, 5, 9, "Docker"),
              ("Portability", 6, 9, 7, "K8s"),
          ]],
        [Paragraph("<b>TOTAL</b>", styles["TableCellLeft"]),
         Paragraph("<b>74</b>", styles["TableCell"]),
         Paragraph("<b>71</b>", styles["TableCell"]),
         Paragraph("<b>65</b>", styles["TableCell"]),
         Paragraph("<b>Cloud Run</b>", styles["TableCell"])],
    ]
    t = Table(score_data, colWidths=[110, 70, 50, 50, 75])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
        ("BACKGROUND", (0, -1), (-1, -1), BLUE_LIGHT),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))

    story.append(PageBreak())

    # ── Cost curves ──
    story.append(Paragraph("3.2 Cost Analysis", styles["SubSection"]))
    story.append(make_cost_curve_chart())
    story.append(Paragraph(
        "Figure 2: Estimated monthly cost vs daily request volume. Cloud Run is cheaper below "
        "~50K requests/day. The crossover with K8s occurs around 40-60K requests/day for this "
        "workload profile. On-prem is cheapest at scale but excludes hardware amortization.",
        styles["Caption"],
    ))

    story.append(Paragraph(
        "For the FAO/AIP workload profile \u2014 <b>moderate, bursty traffic</b> with significant "
        "idle periods (nights, weekends, non-campaign seasons) \u2014 Cloud Run's pay-per-use model "
        "is significantly more economical. A 3-node GKE cluster costs $350-500/month even when idle. "
        "Cloud Run with min-instances=1 for the API service costs approximately $50-80/month at "
        "current traffic levels.",
        styles["Body"],
    ))

    cost_table = [
        [Paragraph("<b>Cost Component</b>", styles["TableHeader"]),
         Paragraph("<b>Cloud Run</b>", styles["TableHeader"]),
         Paragraph("<b>Kubernetes (GKE)</b>", styles["TableHeader"])],
        [Paragraph("Compute (idle)", styles["TableCellLeft"]),
         Paragraph("~$0 (scale-to-zero)", styles["TableCell"]),
         Paragraph("$200-500/mo (min 3 nodes)", styles["TableCell"])],
        [Paragraph("Compute (active)", styles["TableCellLeft"]),
         Paragraph("$0.00002400/vCPU-s", styles["TableCell"]),
         Paragraph("Included in node cost", styles["TableCell"])],
        [Paragraph("Cloud SQL", styles["TableCellLeft"]),
         Paragraph("$50-200/mo", styles["TableCell"]),
         Paragraph("$50-200/mo (same)", styles["TableCell"])],
        [Paragraph("Load Balancer", styles["TableCellLeft"]),
         Paragraph("Included", styles["TableCell"]),
         Paragraph("$18+/mo", styles["TableCell"])],
        [Paragraph("GKE Mgmt Fee", styles["TableCellLeft"]),
         Paragraph("N/A", styles["TableCell"]),
         Paragraph("$74.40/mo (Standard)", styles["TableCell"])],
        [Paragraph("Monitoring", styles["TableCellLeft"]),
         Paragraph("Included", styles["TableCell"]),
         Paragraph("$8+/mo (Prometheus stack)", styles["TableCell"])],
        [Paragraph("<b>Est. Monthly Total</b>", styles["TableCellLeft"]),
         Paragraph("<b>$80-250</b>", styles["TableCell"]),
         Paragraph("<b>$400-900</b>", styles["TableCell"])],
    ]
    t = Table(cost_table, colWidths=[120, 130, 160])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
        ("BACKGROUND", (0, -1), (-1, -1), GREEN_LIGHT),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Paragraph(
        "Table: Estimated monthly cost at current FAO/AIP traffic levels (moderate, variable).",
        styles["Caption"],
    ))

    story.append(PageBreak())
    return story


def build_ops_analysis(styles):
    story = []
    story.append(Paragraph("4. Operational Analysis", styles["SubSection"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    # ── Ops burden ──
    story.append(Paragraph("4.1 Infrastructure Maintenance Burden", styles["SubSection"]))
    story.append(make_ops_burden_chart())
    story.append(Paragraph(
        "Figure 3: Estimated monthly hours spent on infrastructure maintenance (not application development). "
        "Kubernetes requires node upgrades, etcd backups, CNI patches, ingress controller updates, "
        "cert-manager renewals, and RBAC audits.",
        styles["Caption"],
    ))

    ops_items = [
        ("Cloud Run (~5 hrs/mo):", "Deploy pipeline maintenance, IAM reviews, Cloud SQL backups verification. "
         "No OS patching, no cluster upgrades, no certificate rotation."),
        ("Kubernetes (~40 hrs/mo):", "Node pool upgrades (monthly), etcd backup verification, CNI plugin updates, "
         "ingress controller patches, cert-manager renewals, PV/PVC monitoring, RBAC audits, "
         "Helm chart maintenance, monitoring stack updates (Prometheus/Grafana)."),
        ("On-Prem Docker (~25 hrs/mo):", "OS patching, Docker daemon updates, backup management, "
         "firewall rules, SSL certificate renewal, disk space monitoring, log rotation."),
    ]
    for title, body in ops_items:
        story.append(Paragraph(f"<b>{title}</b> {body}", styles["Body"]))

    critical = Paragraph(
        "For a team of <b>1-3 engineers</b> responsible for both application development and "
        "infrastructure, the 35-hour/month difference between Cloud Run and Kubernetes represents "
        "<b>~20% of a full-time engineer's capacity</b> redirected from feature development to "
        "infrastructure maintenance.",
        styles["Callout"],
    )
    story.append(ColorBlock(critical, bg_color=ORANGE_LIGHT, border_color=ORANGE_WARN))
    story.append(Spacer(1, 8))

    # ── Team size ──
    story.append(Paragraph("4.2 Team Size vs Platform Recommendation", styles["SubSection"]))
    story.append(make_team_size_chart())
    story.append(Paragraph(
        "Figure 4: Recommended platform based on engineering team size. Kubernetes is only "
        "justified when a dedicated platform/SRE sub-team (2-3 people) can be allocated.",
        styles["Caption"],
    ))

    # ── Cold start ──
    story.append(Paragraph("4.3 Cold Start Impact", styles["SubSection"]))
    story.append(make_cold_start_chart())
    story.append(Paragraph(
        "Figure 5: Cold start latency for the GDAL-based container image (~2-3 GB). "
        "Cloud Run's cold start is the primary tradeoff, mitigated by min-instances.",
        styles["Caption"],
    ))

    story.append(Paragraph(
        "The GDAL base image is the largest contributor to cold start latency on Cloud Run. "
        "Mitigation strategies available today:",
        styles["Body"],
    ))
    mitigations = [
        "Set <b>min-instances=1</b> for API and Worker services (~$30-50/mo additional cost)",
        "Split the Docker image: lightweight API image (no GDAL) + heavy worker image (with GDAL). "
        "The SCOPE-based build system already supports this.",
        "Use <b>Cloud Run startup CPU boost</b> (available since 2024) to accelerate cold starts",
        "Pre-warm with Cloud Scheduler health check pings during business hours",
    ]
    for m in mitigations:
        story.append(Paragraph(f"\u2022 {m}", styles["BulletCustom"]))

    story.append(PageBreak())
    return story


def build_security_compliance(styles):
    story = []
    story.append(Paragraph("5. Security and Compliance", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    story.append(Paragraph(
        "For an FAO/UN workload, compliance and security certifications are critical procurement "
        "requirements. Cloud Run provides these out-of-the-box at no additional effort.",
        styles["Body"],
    ))

    sec_data = [
        [Paragraph("<b>Aspect</b>", styles["TableHeader"]),
         Paragraph("<b>Cloud Run</b>", styles["TableHeader"]),
         Paragraph("<b>Kubernetes</b>", styles["TableHeader"]),
         Paragraph("<b>On-Prem Docker</b>", styles["TableHeader"])],
        *[[Paragraph(a, styles["TableCellLeft"]),
           Paragraph(cr, styles["TableCell"]),
           Paragraph(k, styles["TableCell"]),
           Paragraph(d, styles["TableCell"])]
          for a, cr, k, d in [
              ("Container Isolation", "gVisor sandbox", "Shared kernel*", "Shared kernel"),
              ("Secret Management", "GCP Secret Mgr", "K8s Secrets (base64)", ".env files"),
              ("IAM / RBAC", "GCP IAM (native)", "K8s RBAC (manual)", "OS-level"),
              ("Network Isolation", "VPC + IAM", "NetworkPolicy", "Firewall rules"),
              ("SOC 2 Type II", "Inherited", "Self-audit", "Self-audit"),
              ("ISO 27001", "Inherited", "Self-audit", "Self-audit"),
              ("Encryption at Rest", "Automatic (AES-256)", "Manual config", "Manual config"),
              ("Encryption in Transit", "Automatic (TLS)", "Cert-manager", "Manual certs"),
              ("Audit Logging", "Cloud Audit Logs", "Manual setup", "Manual setup"),
              ("DDoS Protection", "Cloud Armor", "Manual / WAF", "None"),
          ]],
    ]
    t = Table(sec_data, colWidths=[110, 105, 105, 100])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Paragraph(
        "* Kubernetes can use gVisor (GKE Sandbox) or Kata Containers for stronger isolation, "
        "but this requires explicit configuration and increases cost.",
        styles["Caption"],
    ))

    verdict = Paragraph(
        "Cloud Run provides <b>compliance-by-default</b>. Achieving the same security posture on "
        "self-managed Kubernetes would require significant additional effort in certificate management, "
        "secret encryption, audit logging, and network policy configuration \u2014 all of which "
        "require ongoing maintenance.",
        styles["Callout"],
    )
    story.append(ColorBlock(verdict, bg_color=GREEN_LIGHT, border_color=GREEN_OK))

    story.append(PageBreak())
    return story


def build_decision_framework(styles):
    story = []
    story.append(Paragraph("6. Decision Framework", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    story.append(Paragraph("6.1 Weighted Decision Matrix for DynaStore", styles["SubSection"]))
    story.append(make_decision_matrix_chart())
    story.append(Paragraph(
        "Figure 6: Decision factors weighted for the DynaStore/GeoID context. "
        "4 out of 6 factors favour Cloud Run. GPU need and heavy GDAL tasks "
        "are the only factors favouring K8s, and neither is blocking today.",
        styles["Caption"],
    ))

    story.append(Paragraph("6.2 When to Re-evaluate (Migration Triggers)", styles["SubSection"]))
    story.append(Paragraph(
        "The recommendation to stay on Cloud Run is contingent on current conditions. "
        "Re-evaluate when <b>3 or more</b> of these triggers become true:",
        styles["Body"],
    ))

    triggers = [
        ("<b>GPU Processing Needed</b> \u2014 ML inference or GPU-accelerated raster analysis "
         "becomes a requirement. Cloud Run does not support GPUs."),
        ("<b>Sustained High Traffic</b> \u2014 Monthly Cloud Run bill exceeds 2x the equivalent "
         "committed-use GKE cost for 3+ consecutive months."),
        ("<b>Resource Ceiling Hit</b> \u2014 Tasks regularly require >8 vCPU or >32 GiB memory, "
         "exceeding Cloud Run's per-instance limits."),
        ("<b>Cold Start SLA Violation</b> \u2014 Cold start latency violates an SLA, and "
         "min-instances + startup CPU boost are insufficient."),
        ("<b>Service Mesh Required</b> \u2014 Mandatory mTLS, traffic splitting, or circuit "
         "breaking between services is needed."),
        ("<b>Persistent Scratch Disk</b> \u2014 GDAL processing needs temporary disk space "
         "exceeding the memory-backed tmpfs limit."),
        ("<b>Multi-Cloud Mandate</b> \u2014 Regulatory or strategic requirement to run on "
         "multiple cloud providers or hybrid cloud/on-prem."),
    ]
    for t in triggers:
        story.append(Paragraph(f"\u2022 {t}", styles["BulletCustom"]))

    story.append(Spacer(1, 8))

    current = Paragraph(
        "<b>Current trigger status (March 2026):</b> 0 out of 7 triggers are active. "
        "GPU is not needed, traffic is moderate, tasks fit within limits, and the platform "
        "operates within a single GCP project.",
        styles["Callout"],
    )
    story.append(ColorBlock(current, bg_color=BLUE_LIGHT, border_color=BLUE_PRIMARY))

    story.append(PageBreak())
    return story


def build_hybrid_strategy(styles):
    story = []
    story.append(Paragraph("7. Recommended Phased Strategy", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    story.append(Paragraph(
        "Rather than a binary Cloud Run vs Kubernetes decision, adopt a phased approach "
        "that maximises current benefits while preserving future flexibility.",
        styles["Body"],
    ))

    phase_data = [
        [Paragraph("<b>Phase</b>", styles["TableHeader"]),
         Paragraph("<b>Action</b>", styles["TableHeader"]),
         Paragraph("<b>Trigger</b>", styles["TableHeader"]),
         Paragraph("<b>Timeline</b>", styles["TableHeader"])],
        [Paragraph("0 (Current)", styles["TableCellLeft"]),
         Paragraph("Stay on Cloud Run. Set min-instances=1. Split GDAL image from API image.", styles["TableCellLeft"]),
         Paragraph("Default", styles["TableCell"]),
         Paragraph("Now", styles["TableCell"])],
        [Paragraph("1", styles["TableCellLeft"]),
         Paragraph("Add PgBouncer via Cloud SQL Auth Proxy for connection pooling at scale.", styles["TableCellLeft"]),
         Paragraph(">50 concurrent DB connections", styles["TableCell"]),
         Paragraph("6-12 months", styles["TableCell"])],
        [Paragraph("2", styles["TableCellLeft"]),
         Paragraph("Move heavy GDAL tasks to GKE Autopilot while keeping API on Cloud Run.", styles["TableCellLeft"]),
         Paragraph("Tasks hitting 32 GiB limit or needing persistent disk", styles["TableCell"]),
         Paragraph("12-24 months", styles["TableCell"])],
        [Paragraph("3", styles["TableCellLeft"]),
         Paragraph("Full K8s migration (GKE Autopilot) if sustained traffic justifies it.", styles["TableCellLeft"]),
         Paragraph("Monthly Cloud Run bill > 2x GKE committed-use", styles["TableCell"]),
         Paragraph("24+ months", styles["TableCell"])],
        [Paragraph("4", styles["TableCellLeft"]),
         Paragraph("On-prem K8s (k3s/Rancher) for data-sovereignty deployments.", styles["TableCellLeft"]),
         Paragraph("Specific client/country requirement", styles["TableCell"]),
         Paragraph("As needed", styles["TableCell"])],
    ]

    t = Table(phase_data, colWidths=[65, 160, 115, 70])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
        ("BACKGROUND", (0, 1), (-1, 1), GREEN_LIGHT),
        ("ROWBACKGROUNDS", (0, 2), (-1, -1), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Paragraph(
        "Table: Phased migration strategy. Phase 0 (green) is the immediate recommendation.",
        styles["Caption"],
    ))

    story.append(Spacer(1, 12))

    story.append(Paragraph("7.1 Immediate Optimisations (Phase 0)", styles["SubSection"]))
    optimisations = [
        "<b>Split Docker images:</b> Create a lightweight API image without GDAL (~500 MB) for "
        "the API and GeoID services. Keep GDAL only in the worker/task images. This reduces "
        "cold start from ~10s to ~2-3s for API endpoints.",
        "<b>Set min-instances=1</b> for the API service to eliminate cold starts for the first "
        "request. Cost impact: ~$30-50/month.",
        "<b>Enable startup CPU boost</b> on Cloud Run to double CPU during container initialisation.",
        "<b>Review Cloud Run concurrency settings:</b> Current Gunicorn workers=4 inside Cloud Run "
        "may be suboptimal. Consider increasing Cloud Run concurrency to 80-250 and letting "
        "Cloud Run manage request distribution.",
    ]
    for o in optimisations:
        story.append(Paragraph(f"\u2022 {o}", styles["BulletCustom"]))

    story.append(PageBreak())
    return story


def build_conclusion(styles):
    story = []
    story.append(Paragraph("8. Conclusion", styles["SectionTitle"]))
    story.append(HRule(PAGE_W - 2 * MARGIN, BLUE_PRIMARY, 1.5))
    story.append(Spacer(1, 6))

    # Pie chart
    story.append(make_total_score_pie())
    story.append(Paragraph(
        "Figure 7: Overall weighted scores. Cloud Run leads with 74/100.",
        styles["Caption"],
    ))

    story.append(Paragraph(
        "The DynaStore/GeoID platform is <b>well-architected for portability</b>. Its protocol-based "
        "decoupling, entry-point-driven plugin system, and PostgreSQL-native task queue mean that "
        "the application code is genuinely runtime-agnostic. This is a significant architectural "
        "achievement that makes the deployment platform a <b>configuration choice, not a code change</b>.",
        styles["Body"],
    ))

    story.append(Paragraph(
        "Given the current context \u2014 a small engineering team, moderate and variable traffic, "
        "FAO/UN compliance requirements, and no GPU or extreme resource needs \u2014 "
        "<b>Cloud Run is the correct choice today</b>. It delivers:",
        styles["Body"],
    ))

    benefits = [
        "<b>~$300-650/month savings</b> vs equivalent Kubernetes infrastructure",
        "<b>~35 hours/month freed</b> from infrastructure maintenance to feature development",
        "<b>Compliance-by-default</b> (SOC 2, ISO 27001, encryption, audit logging)",
        "<b>Zero-downtime deployments</b> with built-in traffic splitting and rollback",
        "<b>Automatic scaling</b> from zero to peak without capacity planning",
    ]
    for b in benefits:
        story.append(Paragraph(f"\u2022 {b}", styles["BulletCustom"]))

    story.append(Spacer(1, 10))

    final = Paragraph(
        "<b>The right question is not \"should we use Cloud Run or Kubernetes?\" but rather "
        "\"when will conditions change enough to justify the migration cost?\"</b> The architecture "
        "is ready. The triggers are defined. When 3+ triggers activate, migrate. Until then, "
        "invest engineering time in product features, not infrastructure management.",
        styles["Callout"],
    )
    story.append(ColorBlock(final, bg_color=GREEN_LIGHT, border_color=GREEN_OK))

    story.append(Spacer(1, 30))
    story.append(HRule(PAGE_W - 2 * MARGIN, GREY_BORDER))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "Prepared for the FAO Agro-Informatics Platform (AIP) team \u2014 March 2026",
        styles["Footer"],
    ))

    return story


# ── Build the PDF ───────────────────────────────────────────────────────────

def build_pdf(output_path: str):
    doc = BaseDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN + 14,  # room for header
        bottomMargin=MARGIN,
        title="Infrastructure Decision Report: Cloud Run vs Kubernetes",
        author="FAO - Agro-Informatics Platform",
        subject="Deployment strategy evaluation for DynaStore/GeoID",
    )

    # Page templates
    cover_frame = Frame(
        MARGIN, 0, PAGE_W - 2 * MARGIN, PAGE_H,
        id="cover",
    )
    content_frame = Frame(
        MARGIN, MARGIN, PAGE_W - 2 * MARGIN, PAGE_H - 2 * MARGIN - 14,
        id="content",
    )

    doc.addPageTemplates([
        PageTemplate(id="cover", frames=[cover_frame], onPage=cover_page),
        PageTemplate(id="content", frames=[content_frame], onPage=normal_page),
    ])

    styles = build_styles()
    story = []

    story.extend(build_cover(styles))
    story.extend(build_executive_summary(styles))
    story.extend(build_architecture_overview(styles))
    story.extend(build_comparison_charts(styles))
    story.extend(build_ops_analysis(styles))
    story.extend(build_security_compliance(styles))
    story.extend(build_decision_framework(styles))
    story.extend(build_hybrid_strategy(styles))
    story.extend(build_conclusion(styles))

    doc.build(story)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    output = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "Cloud_Run_vs_Kubernetes_Report.pdf",
    )
    build_pdf(output)
