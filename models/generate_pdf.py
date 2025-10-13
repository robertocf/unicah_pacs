import os
import requests
import pydicom
from io import BytesIO
from datetime import datetime
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from flask import send_file, request
from flask_login import current_user

from db import get_db_connection  # ajuste conforme sua estrutura


def gerar_pdf_completo(study_uid):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT p.pat_id,
               split_part(p.pat_name, '^^^^', 1) AS pat_name,
                               CASE 
                WHEN LENGTH(p.pat_birthdate) = 8 
                    AND p.pat_birthdate ~ '^[0-9]{8}$' 
                THEN to_char(to_date(p.pat_birthdate, 'YYYYMMDD'), 'DD/MM/YYYY')
                    ELSE ''
                END as pat_birthdate,
                             case when length(
                  EXTRACT(YEAR FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' anos e ' ||
                  EXTRACT(MONTH FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' meses') > 0 then EXTRACT(YEAR FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' anos e ' ||
                  EXTRACT(MONTH FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' meses' else '' end AS idade,
               to_char(s.study_datetime, 'DD/MM/YYYY HH24:MI:SS') as study_datetime,
               COALESCE(sr.institution, '') as institution,
               p.pat_sex,
               CASE WHEN s.ref_physician IS NULL THEN '' else s.ref_physician end as ref_physician,
               CASE WHEN s.study_desc IS NULL THEN '' else s.study_desc end as study_desc
        FROM patient p
        JOIN study s ON s.patient_fk = p.pk
        JOIN series sr ON sr.study_fk = s.pk
        WHERE s.pk = %s AND sr.modality != 'SR'
    """,
        [study_uid],
    )
    patient_data = cur.fetchone()

    # Buscar endereço e logo da empresa
    company_address = "Endereço não cadastrado"
    company_logo = None
    
    if patient_data or patient_data[5]:
        cur.execute(
            "SELECT organization, address, logo_path FROM organizations_app WHERE LOWER(presentation) = LOWER(%s)",
            [patient_data[5]]
        )
        org_result = cur.fetchone()
        if org_result:
            if org_result[1]:
                company_address = org_result[1]
            if org_result[2]:
                company_logo = os.path.join('static', 'logos', org_result[2])

    # Query para buscar o diretório do estudo
    archive_query = """
        SELECT SPLIT_PART(f.dirpath, E'\\\\', array_length(string_to_array(f.dirpath, E'\\\\'), 1)) AS dirpath
        FROM files fl
        JOIN instance ins ON fl.instance_fk = ins.pk
        JOIN series sr ON ins.series_fk = sr.pk
        JOIN study st ON sr.study_fk = st.pk
        JOIN filesystem f ON f.pk = fl.filesystem_fk
        WHERE st.pk = %s AND sr.modality != 'SR'
        GROUP BY dirpath
    """
    cur.execute(archive_query, [study_uid])
    archive_result = cur.fetchone()
    archive_path = archive_result[0] if archive_result else "archive"

    cur.execute(
        """
        SELECT concat(fl.filepath)
        FROM files fl, patient pt, study st, series sr, instance ins
        WHERE st.patient_fk = pt.pk
          AND sr.study_fk = st.pk
          AND fl.instance_fk = ins.pk
          AND ins.series_fk = sr.pk
          AND sr.modality != 'SR'
          AND st.pk = %s
        ORDER BY fl.created_time ASC
    """,
        [study_uid],
    )
    dicom_files = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not patient_data:
        return "Paciente não encontrado", 404

    dicom_base_url = f"http://10.2.0.10/{archive_path}/"
    output_dir = "static/temp"
    os.makedirs(output_dir, exist_ok=True)
    jpg_files = []

    for file_path in dicom_files:
        try:
            dicom_url = f"{dicom_base_url}{file_path}"
            print(f"DEBUG PDF: Tentando acessar URL: {dicom_url}")
            
            # Testar diferentes formatos de autenticação HTTP básica
            auth = ('suporte_image', '$apr1$PefDLttp$C.smY/9DZ9PB4ZYaRmria0')
            print(f"DEBUG PDF: Usando autenticação: {auth[0]}")
            
            response = requests.get(dicom_url, auth=auth, timeout=10)
            print(f"DEBUG PDF: Status da resposta: {response.status_code}")
            
            if response.status_code == 401:
                print("DEBUG PDF: Erro 401 - Tentando com credenciais alternativas")
                auth_alt = ('suporte_image', 'suporte123')
                response = requests.get(dicom_url, auth=auth_alt, timeout=10)
                print(f"DEBUG PDF: Status com credenciais alternativas: {response.status_code}")
            
            if response.status_code != 200:
                continue
            ds = pydicom.dcmread(BytesIO(response.content))
            if "PixelData" not in ds:
                continue
            pixel_array = ds.pixel_array
            pixel_array = (pixel_array / pixel_array.max() * 255).astype("uint8")
            img = Image.fromarray(pixel_array)
            file_name = os.path.basename(file_path)
            jpg_path = os.path.join(output_dir, f"{file_name}.jpg")
            img.save(jpg_path, "JPEG", quality=95)
            jpg_files.append(jpg_path)
        except Exception as e:
            print(f"Erro ao processar {file_path}: {e}")

    if not jpg_files:
        return "Nenhum arquivo DICOM válido encontrado", 404

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    logo_path = "static/logo_unicah.png"
    layout = request.args.get('layout', '2x3')
    if layout == "1x1":
        top_margin = 610
        images_per_page = 1
        rows = 1
        cols = 1
        row_spacing = 2
        img_height = 500
        img_width = 575
    elif layout == "2x2":
        top_margin = 450
        images_per_page = 4
        rows = 2
        cols = 2
        row_spacing = 2
        img_height = 680 / rows
        img_width = (width - 30) / cols
    else:  # padrão 2x3
        top_margin = 330
        images_per_page = 6
        rows = 3
        cols = 2
        row_spacing = 5
        img_height = 650 / rows
        img_width = (width - 30) / cols
    total_pages = (len(jpg_files) + images_per_page - 1) // images_per_page
    current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    c.setFont("Helvetica", 6)
    c.drawString(455, height - 10, f"Documento impresso em: {current_time}")
    for i, jpg in enumerate(jpg_files):
        if i % images_per_page == 0:
            if i != 0:
                c.showPage()
            c.setFont("Times-Bold", 10)
            c.setTitle(f"Imagem de {patient_data[1]}")
            c.drawString(170, height - 20, f"ID:")
            c.drawString(170, height - 35, f"Paciente:")
            c.drawString(170, height - 50, f"Nasc:")
            c.drawString(260, height - 50, f"Sexo:")
            c.drawString(310, height - 50, f"Idade:")
            c.drawString(310, height - 65, f"Solicitante:")
            c.drawString(170, height - 65, f"Estudo:")
            c.drawString(170, height - 80, f"Procedimento:")
            c.setFont("Times-Roman", 10)
            c.drawString(186, height - 20, f"{patient_data[0]}")
            c.drawString(211, height - 35, f"{patient_data[1]}")
            c.drawString(195, height - 50, f"{patient_data[2]}")
            c.drawString(340, height - 50, f"{patient_data[3]}")
            c.drawString(205, height - 65, f"{patient_data[4]}")
            c.drawString(285, height - 50, f"{patient_data[6]}")
            c.drawString(360, height - 65, f"{patient_data[7]}")
            c.drawString(235, height - 80, f"{patient_data[8]}")
            # Usar a logo da empresa se disponível, caso contrário usar a logo padrão
            logo_to_use = company_logo if company_logo and os.path.exists(company_logo) else logo_path
            c.drawImage(logo_to_use, width - 585, height - 60, width=150, height=50)
            c.setFont("Times-Roman", 9)
            # Calcula a largura do texto do endereço
            address_width = c.stringWidth(company_address, "Times-Roman", 9)
            # Calcula a posição x para centralizar
            x_position = (width - address_width) / 2
            c.drawString(x_position, 15, company_address)
            c.drawString(270, 30, f"página {i // images_per_page + 1} de {total_pages}")

        row = (i % images_per_page) // cols
        col = i % cols
        x = 10 + col * (img_width + 10)
        y = height - top_margin - (row * (img_height + row_spacing))
        c.drawImage(ImageReader(jpg), x, y, img_width, img_height)

    c.showPage()
    c.save()

    for jpg in jpg_files:
        try:
            os.remove(jpg)
        except:
            pass

    # Atualizar informações no banco
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE study 
            SET study_custom1 = 'I', 
                study_custom2 = to_char(NOW()- '1 hour'::interval, 'DD/MM/YYYY HH24:MI:SS'),
                study_custom3 = %s 
            WHERE pk = %s""",
            (current_user.name, study_uid)
        )
        conn.commit()
    except Exception as e:
        print(f"Erro ao atualizar estudo: {str(e)}")
    finally:
        cur.close()
        conn.close()

    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=False,
        download_name=f"relatorio_{patient_data[1]}.pdf",
        mimetype="application/pdf",
    )
