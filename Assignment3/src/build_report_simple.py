#!/usr/bin/env python3
"""Generate report.pdf from report_content.md using a simpler approach."""

import sys
from pathlib import Path

try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
except ImportError as e:
    print(f"ERROR: {e}")
    sys.exit(1)

def build_pdf(content_file, output_file):
    """Build PDF from markdown content with basic formatting."""
    
    content = Path(content_file).read_text(encoding='utf-8')
    
    doc = SimpleDocTemplate(
        output_file,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch,
    )
    
    styles = getSampleStyleSheet()
    
    # Create custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#0066cc'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold',
    )
    
    h2_style = ParagraphStyle(
        'CustomHeading2',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=colors.HexColor('#0066cc'),
        spaceAfter=8,
        spaceBefore=12,
        fontName='Helvetica-Bold',
    )
    
    h3_style = ParagraphStyle(
        'CustomHeading3',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.HexColor('#333333'),
        spaceAfter=6,
        spaceBefore=8,
        fontName='Helvetica-Bold',
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=11,
        leading=14,
        alignment=TA_JUSTIFY,
        spaceAfter=6,
    )
    
    story = []
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
            story.append(Spacer(1, 0.08*inch))
            i += 1
            continue
        
        # Code blocks
        if line.strip().startswith('```'):
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('```'):
                code_lines.append(lines[i])
                i += 1
            code_text = '<br/>'.join(code_lines).strip()
            story.append(Paragraph(f'<font face="Courier" size="9">{code_text}</font>', body_style))
            story.append(Spacer(1, 0.1*inch))
            i += 1
            continue
        
        # Tables
        if '|' in line:
            table_rows = []
            while i < len(lines) and '|' in lines[i]:
                cells = [c.strip() for c in lines[i].split('|')[1:-1]]
                if cells:
                    table_rows.append(cells)
                i += 1
            
            # Skip separator
            if table_rows and all(c.startswith('-') or c.startswith(':') for c in table_rows[0]):
                table_rows = table_rows[1:]
            
            if table_rows:
                col_count = len(table_rows[0])
                table = Table(table_rows, colWidths=[letter[0]/col_count - 0.5]*col_count)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f0f0f0')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                ]))
                story.append(table)
                story.append(Spacer(1, 0.15*inch))
            continue
        
        # Regular paragraphs
        story.append(Paragraph(line, body_style))
        i += 1
    
    # Build
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
