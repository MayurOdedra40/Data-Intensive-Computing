#!/usr/bin/env python3
"""Generate report.pdf from report_content.md with architecture diagram."""

import sys
import os
from pathlib import Path

# Ensure we use the venv
sys.path.insert(0, '/home/paisie/DIC/.env/lib/python3.10/site-packages')

try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
except ImportError as e:
    print(f"ERROR: reportlab not installed: {e}")
    print("Run: /home/paisie/DIC/.env/bin/python3 -m pip install reportlab")
    sys.exit(1)

def create_architecture_diagram():
    """Create a simple architecture diagram as SVG, then convert to image."""
    # For now, we'll create a text-based diagram
    # In production, use graphviz or draw.io export
    svg_content = '''<?xml version="1.0" encoding="UTF-8"?>
<svg width="600" height="400" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <style>
      .box { fill: #e8f4f8; stroke: #0066cc; stroke-width: 2; }
      .label { font-family: Arial; font-size: 12px; text-anchor: middle; }
      .arrow { stroke: #333; stroke-width: 2; marker-end: url(#arrowhead); }
    </style>
    <marker id="arrowhead" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto">
      <polygon points="0 0, 10 3, 0 6" fill="#333" />
    </marker>
  </defs>
  
  <!-- Boxes -->
  <rect class="box" x="20" y="20" width="120" height="50" rx="5"/>
  <text class="label" x="80" y="50">reviews-ingest</text>
  
  <rect class="box" x="20" y="100" width="120" height="50" rx="5"/>
  <text class="label" x="80" y="130">L1 Preprocess</text>
  
  <rect class="box" x="200" y="100" width="120" height="50" rx="5"/>
  <text class="label" x="260" y="130">L2 Profanity</text>
  
  <rect class="box" x="380" y="100" width="120" height="50" rx="5"/>
  <text class="label" x="440" y="130">L3 Sentiment</text>
  
  <rect class="box" x="20" y="200" width="120" height="50" rx="5"/>
  <text class="label" x="80" y="230">L4 Aggregate</text>
  
  <rect class="box" x="200" y="200" width="120" height="50" rx="5"/>
  <text class="label" x="260" y="230">DynamoDB</text>
  
  <rect class="box" x="380" y="200" width="120" height="50" rx="5"/>
  <text class="label" x="440" y="230">L5 Report</text>
  
  <!-- Arrows (S3 event triggers) -->
  <path class="arrow" d="M 80 70 L 80 100"/>
  <text class="label" x="100" y="87">S3 event</text>
  
  <path class="arrow" d="M 140 125 L 200 125"/>
  <text class="label" x="170" y="115">S3 write</text>
  
  <path class="arrow" d="M 320 125 L 380 125"/>
  <text class="label" x="350" y="115">S3 write</text>
  
  <path class="arrow" d="M 440 150 L 80 200"/>
  <text class="label" x="260" y="180">S3 write</text>
  
  <path class="arrow" d="M 140 225 L 200 225"/>
  <text class="label" x="170" y="215">DynamoDB</text>
  
  <path class="arrow" d="M 320 225 L 380 225"/>
  <text class="label" x="350" y="215">on-demand</text>
</svg>'''
    
    svg_path = Path("/tmp/architecture.svg")
    svg_path.write_text(svg_content)
    return str(svg_path)

def build_pdf(content_file, output_file):
    """Build PDF from markdown content."""
    
    # Read the markdown content
    content = Path(content_file).read_text(encoding='utf-8')
    
    # Create PDF document with 11pt as base
    doc = SimpleDocTemplate(
        output_file,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch,
        title="Assignment 3: Event-Driven Serverless Review Processing Pipeline",
    )
    
    # Define styles
    styles = getSampleStyleSheet()
    
    # Override base font size to 11pt
    styles.normal.fontSize = 11
    styles.normal.leading = 14
    
    # Heading 1: Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#0066cc'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold',
    )
    
    # Heading 2: Section
    h2_style = ParagraphStyle(
        'CustomHeading2',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=colors.HexColor('#0066cc'),
        spaceAfter=8,
        spaceBefore=12,
        fontName='Helvetica-Bold',
    )
    
    # Heading 3: Subsection
    h3_style = ParagraphStyle(
        'CustomHeading3',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.HexColor('#333333'),
        spaceAfter=6,
        spaceBefore=8,
        fontName='Helvetica-Bold',
    )
    
    # Body text
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=11,
        leading=14,
        alignment=TA_JUSTIFY,
        spaceAfter=6,
    )
    
    # Code/table style
    table_style = ParagraphStyle(
        'TableContent',
        parent=styles['BodyText'],
        fontSize=10,
        leading=12,
        fontName='Courier',
    )
    
    story = []
    
    # Parse and format the markdown
    lines = content.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Title
        if line.startswith('# ') and not line.startswith('## '):
            story.append(Paragraph(line[2:], title_style))
            story.append(Spacer(1, 0.15*inch))
            i += 1
            continue
        
        # Heading 2
        if line.startswith('## '):
            story.append(Paragraph(line[3:], h2_style))
            i += 1
            continue
        
        # Heading 3
        if line.startswith('### '):
            story.append(Paragraph(line[4:], h3_style))
            i += 1
            continue
        
        # Empty line
        if not line.strip():
            story.append(Spacer(1, 0.1*inch))
            i += 1
            continue
        
        # Code blocks
        if line.strip().startswith('```'):
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('```'):
                code_lines.append(lines[i])
                i += 1
            code_text = '\n'.join(code_lines).strip()
            story.append(Paragraph(f'<font face="Courier" size="9">{code_text}</font>', body_style))
            story.append(Spacer(1, 0.1*inch))
            i += 1
            continue
        
        # Tables (simple markdown tables)
        if '|' in line:
            # Collect table rows
            table_rows = []
            while i < len(lines) and '|' in lines[i]:
                cells = [c.strip() for c in lines[i].split('|')[1:-1]]
                table_rows.append(cells)
                i += 1
            
            # Skip separator row if present
            if table_rows and all(c.startswith('-') or c.startswith(':') for c in table_rows[0]):
                table_rows = table_rows[1:]
            
            if table_rows:
                table = Table(table_rows, colWidths=[2.0*inch]*len(table_rows[0]))
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                ]))
                story.append(table)
                story.append(Spacer(1, 0.2*inch))
            continue
        
        # Regular paragraphs
        story.append(Paragraph(line, body_style))
        i += 1
    
    # Add page break before results
    if len(story) > 50:
        for j, elem in enumerate(story):
            if hasattr(elem, 'text') and 'Results' in str(elem.text):
                story.insert(j, PageBreak())
                break
    
    # Build the PDF
    doc.build(story)
    print(f"✓ PDF generated: {output_file}")

if __name__ == '__main__':
    src_dir = Path(__file__).parent
    content_file = src_dir / 'report_content.md'
    output_file = src_dir / 'report.pdf'
    
    if not content_file.exists():
        print(f"ERROR: {content_file} not found")
        sys.exit(1)
    
    build_pdf(str(content_file), str(output_file))
