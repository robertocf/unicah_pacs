import requests
import pydicom
import bcrypt
from io import BytesIO
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from flask import send_file, Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_user, logout_user, login_required, current_user, LoginManager, UserMixin
from flask_sqlalchemy import SQLAlchemy
from functools import wraps
from db import get_db_connection
import os
from datetime import datetime
import hashlib
import threading
import socket
from models.homepage import carregar_homepage
from models.generate_pdf import gerar_pdf_completo
from flask import render_template
from pynetdicom import AE, evt, AllStoragePresentationContexts
from werkzeug.utils import secure_filename
import zipfile
import tempfile
import shutil
from config import app, db, login_manager, SERVER_IP, NGINX_AUTH_PASSWORD, NGINX_AUTH_USER
from models.Users import User


@app.route('/relatorios', methods=['GET'])
@login_required
def relatorio():
    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')
    diretorio_filtro = request.args.get('diretorio')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Buscar diretórios disponíveis
    cur.execute("SELECT DISTINCT dirpath FROM filesystem ORDER BY dirpath")
    diretorios_disponiveis = [row[0] for row in cur.fetchall()]
    
    # Filtros de data e diretório para ambas as queries
    filtro_data = ''
    params = [current_user.pk]
    if data_inicio:
        filtro_data += ' AND st.study_datetime >= %s'
        params.append(data_inicio)
    if data_fim:
        filtro_data += ' AND st.study_datetime <= %s'
        params.append(data_fim)
    if diretorio_filtro:
        filtro_data += ' AND f2.dirpath = %s'
        params.append(diretorio_filtro)
    
    # Query de resumo (com filtro de empresa do usuário logado)
    if current_user.role == 'admin':
        query_resumo = f'''
            SELECT COUNT(DISTINCT st.pk) as total_estudos,
                   COUNT(DISTINCT p.pk) as total_pacientes,
                   ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024 / 1024, 2) as total_gb,
                   ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024, 2) as total_mb,
                   COUNT(DISTINCT CASE WHEN p.pat_sex = 'F' THEN p.pk END) as qtd_fem,
                   COUNT(DISTINCT CASE WHEN p.pat_sex = 'M' THEN p.pk END) as qtd_masc,
                   AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(p.pat_birthdate, 'YYYYMMDD')))) as media_idade
            FROM study st
            JOIN patient p ON st.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = st.pk
            JOIN instance ins ON ins.series_fk = sr.pk
            JOIN files f ON f.instance_fk = ins.pk
            JOIN filesystem f2 ON f.filesystem_fk = f2.pk
            WHERE sr.modality != 'SR' {filtro_data}
        '''
        # Remove o user_id dos parâmetros para admin
        params_resumo = params[1:] if params and params[0] == current_user.pk else params
    else:
        query_resumo = f'''
            SELECT COUNT(DISTINCT st.pk) as total_estudos,
                   COUNT(DISTINCT p.pk) as total_pacientes,
                   ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024 / 1024, 2) as total_gb,
                   ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024, 2) as total_mb,
                   COUNT(DISTINCT CASE WHEN p.pat_sex = 'F' THEN p.pk END) as qtd_fem,
                   COUNT(DISTINCT CASE WHEN p.pat_sex = 'M' THEN p.pk END) as qtd_masc,
                   AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(p.pat_birthdate, 'YYYYMMDD')))) as media_idade
            FROM study st
            JOIN patient p ON st.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = st.pk
            JOIN instance ins ON ins.series_fk = sr.pk
            JOIN files f ON f.instance_fk = ins.pk
            JOIN filesystem f2 ON f.filesystem_fk = f2.pk
            WHERE sr.modality != 'SR'
            AND sr.institution IN (
                SELECT oa.presentation 
                FROM organizations_app oa, user_organizations uo 
                WHERE oa.pk = uo.organization_id AND uo.user_id = %s
            ) {filtro_data}
        '''
        params_resumo = params
    
    cur.execute(query_resumo, params_resumo)
    resumo = cur.fetchone()
    total_estudos = resumo[0] or 0
    total_pacientes = resumo[1] or 0
    total_gb = resumo[2] or 0
    total_mb = resumo[3] or 0
    qtd_fem = resumo[4] or 0
    qtd_masc = resumo[5] or 0
    media_idade = round(resumo[6] or 0, 1)
    perc_fem = round((qtd_fem / total_pacientes * 100), 1) if total_pacientes else 0
    perc_masc = round((qtd_masc / total_pacientes * 100), 1) if total_pacientes else 0
    # Query detalhada por paciente
    if current_user.role == 'admin':
        query_detalhada = f'''
            SELECT p.pat_id, split_part(p.pat_name, '^', 1) as pat_name,
                   COUNT(DISTINCT st.pk) as qtd_estudos,
                   ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024, 2) as qtd_mb,
                   array_agg(DISTINCT f2.dirpath) as diretorios,
                   array_agg(DISTINCT CONCAT('http://{SERVER_IP}/', SPLIT_PART(f2.dirpath, E'\\\\', array_length(string_to_array(f2.dirpath, E'\\\\'), 1)), '/', f.filepath)) as caminhos_completos
            FROM study st
            JOIN patient p ON st.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = st.pk
            JOIN instance ins ON ins.series_fk = sr.pk
            JOIN files f ON f.instance_fk = ins.pk
            JOIN filesystem f2 ON f.filesystem_fk = f2.pk
            WHERE sr.modality != 'SR' {filtro_data}
            GROUP BY p.pat_id, pat_name
            ORDER BY qtd_mb DESC
        '''
        # Remove o user_id dos parâmetros para admin
        params_detalhada = params[1:] if params and params[0] == current_user.pk else params
    else:
        query_detalhada = f'''
            SELECT p.pat_id, split_part(p.pat_name, '^', 1) as pat_name,
                   COUNT(DISTINCT st.pk) as qtd_estudos,
                   ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024, 2) as qtd_mb,
                   array_agg(DISTINCT f2.dirpath) as diretorios,
                   array_agg(DISTINCT CONCAT('http://{SERVER_IP}/', SPLIT_PART(f2.dirpath, E'\\\\', array_length(string_to_array(f2.dirpath, E'\\\\'), 1)), '/', f.filepath)) as caminhos_completos
            FROM study st
            JOIN patient p ON st.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = st.pk
            JOIN instance ins ON ins.series_fk = sr.pk
            JOIN files f ON f.instance_fk = ins.pk
            JOIN filesystem f2 ON f.filesystem_fk = f2.pk
            WHERE sr.modality != 'SR'
            AND sr.institution IN (
                SELECT oa.presentation 
                FROM organizations_app oa, user_organizations uo 
                WHERE oa.pk = uo.organization_id AND uo.user_id = %s
            ) {filtro_data}
            GROUP BY p.pat_id, pat_name
            ORDER BY qtd_mb DESC
        '''
        params_detalhada = params
    
    cur.execute(query_detalhada, params_detalhada)
    pacientes = [
        {
            'pat_id': row[0],
            'pat_name': row[1],
            'qtd_estudos': row[2],
            'qtd_mb': row[3],
            'diretorios': row[4],
            'caminhos_completos': row[5] if len(row) > 5 else []
        }
        for row in cur.fetchall()
    ]
    cur.close()
    conn.close()
    return render_template('relatorio.html',
        data_inicio=data_inicio or '',
        data_fim=data_fim or '',
        diretorio_filtro=diretorio_filtro or '',
        diretorios_disponiveis=diretorios_disponiveis,
        total_estudos=total_estudos,
        total_pacientes=total_pacientes,
        total_gb=total_gb,
        total_mb=total_mb,
        perc_fem=perc_fem,
        perc_masc=perc_masc,
        media_idade=media_idade,
        pacientes=pacientes
    )

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'admin':
            return redirect(url_for('homepage', alert='access_denied'))
        return f(*args, **kwargs)
    return decorated_function

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

def handle_store(event):
    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        if not hasattr(ds, "PatientID") or not hasattr(ds, "StudyInstanceUID") or not hasattr(ds, "SOPInstanceUID"):
            print("DICOM incompleto recebido. Ignorando.")
            return 0xA900
        patient_id = ds.PatientID
        study_uid = ds.StudyInstanceUID
        modality = getattr(ds, "Modality", "OT")
        dir_path = os.path.join("impressos", study_uid)
        os.makedirs(dir_path, exist_ok=True)
        filename = os.path.join(dir_path, f"{ds.SOPInstanceUID}.dcm")
        ds.save_as(filename, write_like_original=False)
        print(f"[RECEBIDO] {filename}")
        salvar_no_banco(patient_id, study_uid, modality, filename)  # Chamada única
        return 0x0000
    except Exception as e:
        print(f"[ERRO] Falha no handle_store: {str(e)}")
        return 0xC000

def iniciar_dicom_server():
    ae = AE()
    ae.supported_contexts = AllStoragePresentationContexts
    handlers = [(evt.EVT_C_STORE, handle_store)]
    ae.start_server(("0.0.0.0", 104), evt_handlers=handlers)

dicom_thread = threading.Thread(target=iniciar_dicom_server, daemon=True)
dicom_thread.start()

def salvar_no_banco(patient_id, study_uid, modality, path):
    filme_tipo = "Dry Film"
    filme_tamanho = "14x17"
    formato = "3x2"
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO impressoes (patient_id, study_uid, modality, filme_tipo, filme_tamanho, formato, path)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (patient_id, study_uid, modality, filme_tipo, filme_tamanho, formato, path),
    )
    conn.commit()
    cur.close()
    conn.close()

@app.route("/", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("homepage"))
    if request.method == "POST":
        user_id = request.form["username"]
        password = request.form["password"]
        
        # Buscar usuário com consulta SQL direta para verificar se está ativo
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT pk, user_id, password, name, role, active FROM users_app WHERE user_id = %s', (user_id,))
        user_data = cur.fetchone()
        cur.close()
        conn.close()
        
        if user_data and bcrypt.checkpw(password.encode("utf-8"), user_data[2].encode("utf-8")):
            # Verificar se o usuário está ativo
            if not user_data[5]:  # active é o índice 5
                return render_template("login.html", erro="Usuário bloqueado. Entre em contato com o administrador.")
            
            # Buscar o usuário pelo SQLAlchemy para fazer login
            user = User.query.filter_by(user_id=user_id).first()
            login_user(user)
            return redirect(url_for("homepage"))
        return render_template("login.html", erro="Usuário ou senha inválidos")
    return render_template("login.html")

@app.route('/meu_perfil/atualizar', methods=['POST'])
@login_required
def atualizar_perfil():
    data = request.get_json()
    user = User.query.get(current_user.pk)
    if not user:
        return jsonify({'success': False, 'message': 'Usuário não encontrado'})
    # Apenas atualiza a senha, não o nome ou outros campos
    if 'password' in data and data['password']:
        hashed = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt())
        user.password = hashed.decode('utf-8')
        try:
            db.session.commit()
            return jsonify({'success': True, 'message': 'Senha atualizada com sucesso'})
        except Exception as e:
            db.session.rollback()
            return jsonify({'success': False, 'message': str(e)})
    else:
        return jsonify({'success': False, 'message': 'Nenhuma alteração realizada'})

@app.route("/home", methods=["GET", "POST"])
@login_required
def homepage():
    alert = request.args.get('alert')
    return carregar_homepage(user_name=current_user.name, user_id=current_user.pk, user_role=current_user.role, alert=alert)

@app.route("/generate_pdf/<study_uid>")
@login_required
def generate_pdf(study_uid):
    return gerar_pdf_completo(study_uid)

@app.route("/laudo")
@login_required
@admin_required
def editor():
    protocolo = request.args.get("protocolo")
    if not protocolo:
        return "Protocolo não fornecido", 400
    return render_template("editor.html")

@app.route("/select_images/<study_uid>")
@login_required
def select_images(study_uid):
    conn = get_db_connection()
    cur = conn.cursor()
    
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
    
    query = """
        SELECT concat(fl.filepath) AS caminho
        FROM files fl
        JOIN instance ins ON fl.instance_fk = ins.pk
        JOIN series sr ON ins.series_fk = sr.pk
        JOIN study st ON sr.study_fk = st.pk
        WHERE st.pk = %s AND sr.modality != 'SR'
        ORDER BY fl.created_time ASC
    """
    cur.execute(query, [study_uid])
    dicom_files = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    dicom_base_url = f"http://{SERVER_IP}/{archive_path}/"
    return render_template(
        "select_images.html",
        dicom_files=dicom_files,
        study_uid=study_uid,
        dicom_base_url=dicom_base_url,
    )

@app.route("/deletar_paciente", methods=["POST"])
@login_required
@admin_required
def deletar_paciente():
    data = request.get_json()
    pat_id = data["pat_id"]
    pat_name = data["pat_name"]
    pat_birthdate = data["pat_birthdate"]
    pat_sex = data["pat_sex"]
    if pat_birthdate == "None":
        pat_birthdate = ""
    hl7_msg = f"""MSH|^~\\&|SISTEMA_ORIGEM|HOSPITAL_X|DCM4CHEE|DCM4CHEE|{datetime.now().strftime('%Y%m%d%H%M%S')}||ADT^A23|MSG_{pat_id}|P|2.3
EVN|A23|{datetime.now().strftime('%Y%m%d%H%M%S')}
PID|1||{pat_id}^^^||{pat_name}^^^||{pat_birthdate.replace('-','')}|{pat_sex}||"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("{SERVER_IP}", 6663))
            s.sendall(b"\x0b" + hl7_msg.encode() + b"\x1c\x0d")
        return jsonify({"message": "Paciente excluído com sucesso!"}), 200
    except Exception as e:
        return jsonify({"message": f"Erro ao enviar mensagem HL7: {e}"}), 500

@app.route("/generate_selected_pdf/<study_uid>", methods=["POST"])
@login_required
def generate_selected_pdf(study_uid):
    selected_files = request.form.getlist("selected_files")
    if not selected_files:
        return "Nenhuma imagem selecionada.", 400
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.pat_id,
               split_part(p.pat_name, '^^^^', 1) AS pat_name,
               CASE 
                   WHEN LENGTH(p.pat_birthdate) = 8 AND p.pat_birthdate ~ '^[0-9]{8}$' 
                   THEN to_char(to_date(p.pat_birthdate, 'YYYYMMDD'), 'DD/MM/YYYY')
                   ELSE ''
               END as pat_birthdate,
               CASE WHEN LENGTH(
                   EXTRACT(YEAR FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' anos e ' ||
                   EXTRACT(MONTH FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' meses') > 0 
                   THEN EXTRACT(YEAR FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' anos e ' ||
                   EXTRACT(MONTH FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' meses' 
                   ELSE '' 
               END AS idade,
               to_char(s.study_datetime, 'DD/MM/YYYY HH24:MI:SS') as study_datetime,
               COALESCE(sr.institution, '') as institution,
               p.pat_sex,
               CASE WHEN s.ref_physician IS NULL THEN '' else s.ref_physician end as ref_physician,
               CASE WHEN s.study_desc IS NULL THEN '' else s.study_desc end as study_desc
        FROM patient p
        JOIN study s ON s.patient_fk = p.pk
        JOIN series sr ON sr.study_fk = s.pk
        WHERE s.pk = %s
        LIMIT 1
        """,
        [study_uid],
    )
    patient_data = cur.fetchone()
    company_address = "Endereço não cadastrado"
    company_logo = None
    logo_path = "static/logo_unicah.png"  # Logo padrão

    if patient_data and patient_data[5] != '': 
        cur.execute(
            "SELECT organization, address, logo_path FROM organizations_app WHERE LOWER(presentation) = LOWER(%s)",
            (patient_data[5],)
        )
        org_result = cur.fetchone()        
        if org_result:
            company_address = org_result[1] or company_address
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
    
    cur.close()
    conn.close()
    if not patient_data:
        return "Dados do paciente não encontrados", 404
    dicom_base_url = f"http://{SERVER_IP}/{archive_path}/"
    output_dir = "static/temp"
    os.makedirs(output_dir, exist_ok=True)
    converted_image_paths = []
    for file_path in selected_files:
        try:
            dicom_url = f"{dicom_base_url}{file_path}"
            print(f"DEBUG: Tentando acessar URL: {dicom_url}")
            
            # Testar diferentes formatos de autenticação HTTP básica
            # Formato 1: Credenciais diretas
            auth = (f'{NGINX_AUTH_USER}', f'{NGINX_AUTH_PASSWORD}')
            print(f"DEBUG: Usando autenticação: {auth[0]}")
            
            response = requests.get(dicom_url, auth=auth, timeout=10)
            print(f"DEBUG: Status da resposta: {response.status_code}")
            print(f"DEBUG: Headers da resposta: {dict(response.headers)}")
            
            if response.status_code == 401:
                print("DEBUG: Erro 401 - Tentando com credenciais alternativas")
                # Tentar com senha em texto plano
                auth_alt = (f'{NGINX_AUTH_USER}', f'{NGINX_AUTH_PASSWORD}')
                response = requests.get(dicom_url, auth=auth_alt, timeout=10)
                print(f"DEBUG: Status com credenciais alternativas: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Erro ao baixar {file_path}: {response.status_code}")
                continue
            ds = pydicom.dcmread(BytesIO(response.content))
            if "PixelData" not in ds:
                print(f"{file_path} não contém imagem.")
                continue
            pixel_array = ds.pixel_array
            pixel_array = (pixel_array / pixel_array.max() * 255).astype("uint8")
            img = Image.fromarray(pixel_array)
            file_name = os.path.basename(file_path)
            jpg_path = os.path.join(output_dir, f"{file_name}.jpg")
            img.save(jpg_path, "JPEG", quality=95)
            converted_image_paths.append(jpg_path)
        except Exception as e:
            print(f"Erro convertendo {file_path}: {str(e)}")
    if not converted_image_paths:
        return "Falha ao converter as imagens selecionadas.", 500
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    layout = request.form.get("layout", "2x3")
    
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
    total_pages = (len(converted_image_paths) + images_per_page - 1) // images_per_page
    current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    c.setFont("Times-Roman", 6)
    c.drawString(455, height - 10, f"Documento impresso em: {current_time}")
    for i, jpg in enumerate(converted_image_paths):
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
            logo_to_use = company_logo if company_logo and os.path.exists(company_logo) else logo_path
            c.drawImage(logo_to_use, width - 585, height - 60, width=150, height=50)
            c.setFont("Times-Roman", 9)
            address_width = c.stringWidth(company_address, "Helvetica", 9)
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
    for jpg in converted_image_paths:
        try:
            os.remove(jpg)
        except:
            pass
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

@app.route("/editar_paciente", methods=["POST"])
@login_required
@admin_required
def editar_paciente():
    data = request.get_json()
    pat_id = data["pat_id"]
    pat_name = data["pat_name"]
    pat_birthdate = data["pat_birthdate"]   
    if pat_birthdate == "None":
        pat_birthdate = ""
    if data["pat_sex"] == "":
        pat_sex = '""'
    else:
        pat_sex = data["pat_sex"]      
    hl7_msg = f"""MSH|^~\\&|SISTEMA_ORIGEM|CLINICA ARTUS|DCM4CHEE|DCM4CHEE|{datetime.now().strftime('%Y%m%d%H%M%S')}||ADT^A08|MSG_{pat_id}|P|2.3
EVN|A08|{datetime.now().strftime('%Y%m%d%H%M%S')}
PID|1||{pat_id}^^^||{pat_name}^^^||{pat_birthdate.replace('-','')}|{pat_sex}||"""
    print(hl7_msg)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("{SERVER_IP}", 6662))
            s.sendall(b"\x0b" + hl7_msg.encode() + b"\x1c\x0d")
        return jsonify({"message": "Paciente atualizado com sucesso!"}), 200
    except Exception as e:
        return jsonify({"message": f"Erro ao enviar mensagem HL7: {e}"}), 500

@app.route("/editar_estudo", methods=["POST"])
@login_required
@admin_required
def editar_estudo():
    data = request.get_json()
    pk = data["pk"]
    company = data["company"]
    ref_physician = data["ref_physician"]
    procedure_desc = data["procedure_desc"]
    procedure_code = data["procedure_code"]
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Update na tabela study
        cur.execute("""
            UPDATE study 
            SET ref_physician = %s, study_desc = %s, accession_no = %s
            WHERE pk = %s
        """, (ref_physician, procedure_desc, procedure_code, pk))
        
        # Update na tabela series
        cur.execute("""
            UPDATE series 
            SET institution = %s
            WHERE study_fk = %s
        """, (company, pk))
        
        conn.commit()
        return jsonify({"message": "Estudo atualizado com sucesso!"}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Erro ao atualizar estudo: {e}"}), 500
    finally:
        cur.close()
        conn.close()

@app.route("/teste_study_iuid/<study_pk>")
def get_study_iuid(study_pk):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Primeiro, faz o SELECT
        cur.execute("SELECT study_iuid FROM study WHERE pk = %s", (study_pk,))
        result = cur.fetchone()
        if not result:
            return jsonify({"error": "Study not found"}), 404
        study_iuid = result[0]
        # Faz o UPDATE
        cur.execute(
            """
            UPDATE study
            SET study_custom1 = 'V',
                study_custom2 = to_char(NOW() - INTERVAL '1 hour', 'DD/MM/YYYY HH24:MI:SS'),
                study_custom3 = %s
            WHERE pk = %s
            """,
            (current_user.name, study_pk)
        )
        conn.commit()
        return jsonify({"study_iuid": study_iuid})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route("/thumbnail")
@login_required
def thumbnail():
    path = request.args.get("path")
    study_uid = request.args.get("study_uid")
    if not path:
        return "Caminho não fornecido", 400
    
    # Buscar o archive_path correto do banco de dados
    if study_uid:
        conn = get_db_connection()
        cur = conn.cursor()
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
        cur.close()
        conn.close()
    else:
        # Fallback para o método antigo se study_uid não for fornecido
        path_parts = path.split('/')
        if len(path_parts) > 1:
            archive_path = path_parts[0]
        else:
            archive_path = "archive"
    
    dicom_base_url = f"http://{SERVER_IP}/{archive_path}/"
    dicom_url = f"{dicom_base_url}{path}"
    thumb_dir = "static/thumbnails"
    os.makedirs(thumb_dir, exist_ok=True)
    thumb_name = hashlib.md5(path.encode()).hexdigest() + ".jpg"
    thumb_path = os.path.join(thumb_dir, thumb_name)
    if not os.path.exists(thumb_path):
        try:
            print(f"DEBUG THUMBNAIL: Tentando acessar URL: {dicom_url}")
            
            # Testar diferentes formatos de autenticação HTTP básica
            auth = (f'{NGINX_AUTH_USER}', f'{NGINX_AUTH_PASSWORD}')
            print(f"DEBUG THUMBNAIL: Usando autenticação: {auth[0]}")
            
            response = requests.get(dicom_url, auth=auth, timeout=10)
            print(f"DEBUG THUMBNAIL: Status da resposta: {response.status_code}")
            
            if response.status_code == 401:
                print("DEBUG THUMBNAIL: Erro 401 - Tentando com credenciais alternativas")
                auth_alt = (f'{NGINX_AUTH_USER}', f'{NGINX_AUTH_PASSWORD}')
                response = requests.get(dicom_url, auth=auth_alt, timeout=10)
                print(f"DEBUG THUMBNAIL: Status com credenciais alternativas: {response.status_code}")
            
            if response.status_code != 200:
                return "Erro ao baixar DICOM", 500
            ds = pydicom.dcmread(BytesIO(response.content))
            if "PixelData" not in ds:
                return "DICOM sem imagem", 400
            pixel_array = ds.pixel_array
            pixel_array = (pixel_array / pixel_array.max() * 255).astype("uint8")
            img = Image.fromarray(pixel_array)
            img.thumbnail((200, 200))
            img.save(thumb_path, "JPEG", quality=85)
        except Exception as e:
            return f"Erro: {str(e)}", 500
    response = send_file(thumb_path, mimetype="image/jpeg")
    # Headers de segurança para evitar cache e exposição
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/configuracoes/armazenamento/buscar/<pk>')
@login_required
@admin_required
def buscar_diretorio(pk):
    conn = get_db_connection()
    cur = conn.cursor()

    try:        
        cur.execute("""
            SELECT pk, dirpath, fs_group_id, retrieve_aet, fs_status
            FROM filesystem
            WHERE pk = %s
        """, (pk,))
        diretorio = cur.fetchone()
        if diretorio:
            return jsonify({
                'pk': diretorio[0],
                'dirpath': diretorio[1],
                'fs_group_id': diretorio[2],
                'retrieve_aet': diretorio[3],
                'fs_status': diretorio[4]
            })
        return jsonify({'error': 'Diretório não encontrado'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/armazenamento')
@login_required
@admin_required
def armazenamento():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
SELECT 
    f2.pk,
    f2.dirpath, 
    f2.fs_group_id, 
    f2.retrieve_aet, 
    f2.fs_status, 
    ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024, 2) AS qtd
FROM filesystem f2
LEFT JOIN files f ON f.filesystem_fk = f2.pk
GROUP BY 
    f2.pk,
    f2.dirpath, 
    f2.fs_group_id, 
    f2.fs_status, 
    f2.retrieve_aet
    ORDER BY f2.fs_status
    """)
    directories = [{
        'pk': row[0],
        'dirpath': row[1],
        'fs_group_id': row[2],
        'retrieve_aet': row[3],
        'fs_status': row[4],
        'qtd': row[5]
    } for row in cur.fetchall()]
    cur.close()
    conn.close()

    return render_template('armazenamento.html', directories=directories)

@app.route('/configuracoes/armazenamento/salvar', methods=['POST'])
@login_required
@admin_required
def salvar_armazenamento():
    data = request.get_json()
    dirpath = data['dirpath']
    fs_group_id = data['fs_group_id']
    retrieve_aet = data['retrieve_aet']
    fs_status = data['fs_status']
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if fs_status == 0:
            cur.execute('UPDATE filesystem SET fs_status = 1')
        cur.execute('SELECT pk FROM filesystem WHERE dirpath = %s', (dirpath,))
        existing = cur.fetchone()
        if existing:
            cur.execute("""
                UPDATE filesystem 
                SET fs_group_id = %s, retrieve_aet = %s, fs_status = %s 
                WHERE dirpath = %s
            """, (fs_group_id, retrieve_aet, fs_status, dirpath))
        else:
            cur.execute("""
                INSERT INTO filesystem (dirpath, fs_group_id, retrieve_aet, fs_status, availability) 
                VALUES (%s, %s, %s, %s,0)
            """, (dirpath, fs_group_id, retrieve_aet, fs_status))
        conn.commit()
        return jsonify({'success': True, 'message': 'Diretório salvo com sucesso!'})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': f'Erro ao salvar diretório: {str(e)}'}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/armazenamento/excluir/<int:pk>', methods=['DELETE'])
@login_required
@admin_required
def excluir_armazenamento(pk):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT fs_status FROM filesystem WHERE pk = %s', (pk,))
        result = cur.fetchone()
        if result and result[0] == 0:
            return jsonify({'success': False, 'message': 'Não é possível excluir o diretório principal'}), 400
        cur.execute('SELECT COUNT(*) FROM files f JOIN filesystem fs ON f.filesystem_fk = fs.pk WHERE fs.pk = %s', (pk,))
        count = cur.fetchone()[0]
        if count > 0:
            return jsonify({'success': False, 'message': 'Não é possível excluir um diretório que contém arquivos'}), 400
        cur.execute('DELETE FROM filesystem WHERE pk = %s', (pk,))
        conn.commit()
        return jsonify({'success': True, 'message': 'Diretório excluído com sucesso!'})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': f'Erro ao excluir diretório: {str(e)}'}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/armazenamento/alterar-ordem/<int:pk>', methods=['POST'])
@login_required
@admin_required
def alterar_ordem_armazenamento(pk):
    data = request.get_json()
    acao = data.get('acao')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Buscar o fs_status atual do diretório
        cur.execute('SELECT fs_status FROM filesystem WHERE pk = %s', (pk,))
        result = cur.fetchone()
        if not result:
            return jsonify({'success': False, 'message': 'Diretório não encontrado'}), 404
        
        fs_status_atual = result[0]
        
        if acao == 'subir':
            # Diminuir fs_status (maior prioridade)
            if fs_status_atual == 0:
                return jsonify({'success': False, 'message': 'Este diretório já possui a maior prioridade'}), 400
            
            # Verificar se existe outro diretório com fs_status menor
            novo_fs_status = fs_status_atual - 1
            cur.execute('SELECT pk FROM filesystem WHERE fs_status = %s', (novo_fs_status,))
            conflito = cur.fetchone()
            
            if conflito:
                # Trocar as posições
                cur.execute('UPDATE filesystem SET fs_status = %s WHERE pk = %s', (fs_status_atual, conflito[0]))
            
            cur.execute('UPDATE filesystem SET fs_status = %s WHERE pk = %s', (novo_fs_status, pk))
            
        elif acao == 'descer':
            # Aumentar fs_status (menor prioridade)
            # Buscar o maior fs_status existente
            cur.execute('SELECT MAX(fs_status) FROM filesystem')
            max_fs_status = cur.fetchone()[0] or 0
            
            if fs_status_atual >= max_fs_status:
                return jsonify({'success': False, 'message': 'Este diretório já possui a menor prioridade'}), 400
            
            # Verificar se existe outro diretório com fs_status maior
            novo_fs_status = fs_status_atual + 1
            cur.execute('SELECT pk FROM filesystem WHERE fs_status = %s', (novo_fs_status,))
            conflito = cur.fetchone()
            
            if conflito:
                # Trocar as posições
                cur.execute('UPDATE filesystem SET fs_status = %s WHERE pk = %s', (fs_status_atual, conflito[0]))
            
            cur.execute('UPDATE filesystem SET fs_status = %s WHERE pk = %s', (novo_fs_status, pk))
        
        else:
            return jsonify({'success': False, 'message': 'Ação inválida'}), 400
        
        conn.commit()
        return jsonify({'success': True, 'message': 'Ordem alterada com sucesso!'})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': f'Erro ao alterar ordem: {str(e)}'}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/empresas')
@login_required
@admin_required
def empresas():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT pk, organization, presentation, cnpj, email, phone, active, expiration FROM organizations_app order by pk')
    empresas = [{
        'pk': row[0],
        'organization': row[1],
        'presentation': row[2],
        'cnpj': row[3],
        'email': row[4],
        'phone': row[5],
        'active': row[6],
        'expiration': row[7]
    } for row in cur.fetchall()]
    cur.close()
    conn.close()
    return render_template('empresas.html', empresas=empresas)

@app.route('/configuracoes/empresas/excluir/<int:id>', methods=['DELETE'])
@login_required
@admin_required
def excluir_empresa(id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM organizations_app WHERE pk = %s', (id,))
        conn.commit()
        return jsonify({'success': True, 'message': 'Empresa excluída com sucesso'})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': 'Erro ao excluir empresa'})
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/empresas/cadastrar', methods=['POST'])
@login_required
@admin_required
def cadastrar_empresa():
    organization = request.form['organization']
    presentation = request.form['presentation']
    email = request.form['email']
    phone = request.form['phone']
    cnpj = request.form['cnpj']
    address = request.form['address']
    start_contract = request.form['start_contract']
    expiration = request.form['expiration']
    active = 'active' in request.form
    filename = None
    logo_path = None
    if 'logo' in request.files:
        logo = request.files['logo']
        if logo and logo.filename:
            try:
                img = Image.open(logo)
                if img.size != (402, 127):
                    flash('A imagem deve ter exatamente 402x127 pixels.', 'error')
                    return redirect(url_for('empresas'))
                filename = secure_filename(f"{cnpj}_{logo.filename}")
                logo_path = os.path.join(app.static_folder, 'logos', filename)
                img.save(logo_path)
            except Exception as e:
                flash(f'Erro ao processar a imagem: {str(e)}', 'error')
                return redirect(url_for('empresas'))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            'INSERT INTO organizations_app (organization, presentation, email, phone, cnpj, address, start_contract, expiration, active, logo_path) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (organization, presentation, email, phone, cnpj, address, start_contract, expiration, active, filename)
        )
        conn.commit()
        flash('Empresa cadastrada com sucesso!', 'success')
    except Exception as e:
        conn.rollback()
        if logo_path and os.path.exists(logo_path):
            os.remove(logo_path)
        flash('Erro ao cadastrar empresa. Verifique se o CNPJ já existe.', 'error')
    finally:
        cur.close()
        conn.close()
    return redirect(url_for('empresas'))



@app.route('/configuracoes/empresas/buscar/<int:id>')
@login_required
@admin_required
def buscar_empresa(id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT organization, presentation, email, phone, cnpj, address, start_contract, expiration, active, logo_path FROM organizations_app WHERE pk = %s', (id,))
        empresa = cur.fetchone()
        if empresa:
            # Formatar datas para dd-mm-yyyy ou string vazia se None
            start_contract_formatted = empresa[6].strftime('%d-%m-%Y') if empresa[6] else ''
            expiration_formatted = empresa[7].strftime('%d-%m-%Y') if empresa[7] else ''
            
            return jsonify({
                'organization': empresa[0],
                'presentation': empresa[1],
                'email': empresa[2],
                'phone': empresa[3],
                'cnpj': empresa[4],
                'address': empresa[5],
                'start_contract': start_contract_formatted,
                'expiration': expiration_formatted,
                'active': empresa[8],
                'logo_path': empresa[9]
            })
            
        else:
            return jsonify({'error': 'Empresa não encontrada'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/empresas/editar/<int:id>', methods=['POST'])
@login_required
@admin_required
def editar_empresa(id):
    organization = request.form['organization']
    presentation = request.form['presentation']
    email = request.form['email']
    phone = request.form['phone']
    cnpj = request.form['cnpj']
    address = request.form['address']
    start_contract = request.form['start_contract']
    expiration = request.form['expiration']
    active = 'active' in request.form
    
    # Converter strings vazias para None (NULL no banco)
    start_contract = start_contract if start_contract.strip() else None
    expiration = expiration if expiration.strip() else None
    address = address if address.strip() else None
    
    filename = None
    logo_path = None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT logo_path FROM organizations_app WHERE pk = %s', (id,))
        old_logo = cur.fetchone()
        old_logo_path = old_logo[0] if old_logo else None
        if 'logo' in request.files:
            logo = request.files['logo']
            if logo and logo.filename:
                img = Image.open(logo)
                if img.size != (402, 127):
                    return jsonify({'success': False, 'message': 'A imagem deve ter exatamente 402x127 pixels.'})
                filename = secure_filename(f"{cnpj}_{logo.filename}")
                logo_path = os.path.join(app.static_folder, 'logos', filename)
                img.save(logo_path)
        if filename:
            cur.execute(
                'UPDATE organizations_app SET organization = %s, presentation = %s, email = %s, phone = %s, cnpj = %s, address = %s, start_contract = %s, expiration = %s, active = %s, logo_path = %s WHERE pk = %s',
                (organization, presentation, email, phone, cnpj, address, start_contract, expiration, active, filename, id)
            )
            if old_logo_path:
                old_logo_full_path = os.path.join(app.static_folder, 'logos', old_logo_path)
                if os.path.exists(old_logo_full_path):
                    os.remove(old_logo_full_path)
        else:
            cur.execute(
                'UPDATE organizations_app SET organization = %s, presentation = %s, email = %s, phone = %s, cnpj = %s, address = %s, start_contract = %s, expiration = %s, active = %s WHERE pk = %s',
                (organization, presentation, email, phone, cnpj, address, start_contract, expiration, active, id)
            )
        conn.commit()
        return jsonify({'success': True, 'message': 'Empresa atualizada com sucesso'})
    except Exception as e:
        conn.rollback()
        if logo_path and os.path.exists(logo_path):
            os.remove(logo_path)
        return jsonify({'success': False, 'message': f'Erro ao atualizar empresa: {str(e)}'})
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/usuarios')
@login_required
@admin_required
def usuarios():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT pk, user_id, name, role, active FROM users_app')
    usuarios = [{'id': row[0], 'user_id': row[1], 'nome': row[2], 'grupo': row[3], 'active': row[4]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return render_template('usuarios.html', usuarios=usuarios)

@app.route('/configuracoes/usuarios/cadastrar', methods=['POST'])
@login_required
@admin_required
def cadastrar_usuario():
    user_id = request.form['user_id']
    nome = request.form['nome']
    senha = request.form['senha']
    grupo = request.form['grupo']
    active = 'active' in request.form  # Converte checkbox para boolean
    senha_hash = bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt())
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('INSERT INTO users_app (user_id, name, password, role, active) VALUES (%s, %s, %s, %s, %s)',
                    (user_id, nome, senha_hash.decode('utf-8'), grupo, active))
        conn.commit()
        flash('Usuário cadastrado com sucesso!', 'success')
    except Exception as e:
        conn.rollback()
        flash('Erro ao cadastrar usuário. Verifique se o ID já existe.', 'error')
    finally:
        cur.close()
        conn.close()
    return redirect(url_for('usuarios'))

@app.route('/configuracoes/usuarios/excluir/<int:id>', methods=['DELETE'])
@login_required
@admin_required
def excluir_usuario(id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM users_app WHERE pk = %s', (id,))
        conn.commit()
        return jsonify({'success': True, 'message': 'Usuário excluído com sucesso'})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': 'Erro ao excluir usuário'})
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/usuarios/editar/<int:id>', methods=['POST'])
@login_required
@admin_required
def editar_usuario(id):
    user_id = request.form['user_id']
    nome = request.form['nome']
    senha = request.form.get('senha')
    grupo = request.form['grupo']    
    active = 'active' in request.form  # Converte checkbox para boolean
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if senha and senha.strip():
            senha_hash = bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt())
            cur.execute('UPDATE users_app SET user_id = %s, name = %s, password = %s, role = %s, active = %s WHERE pk = %s',
                        (user_id, nome, senha_hash.decode('utf-8'), grupo, active, id))
        else:
            cur.execute('UPDATE users_app SET user_id = %s, name = %s, role = %s, active = %s WHERE pk = %s',
                        (user_id, nome, grupo, active, id))
        conn.commit()
        return jsonify({'success': True, 'message': 'Usuário atualizado com sucesso'})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': 'Erro ao atualizar usuário'})
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/associacoes')
@login_required
@admin_required
def associacoes():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT pk, user_id, name FROM users_app ORDER BY name')
    usuarios = [{'id': row[0], 'user_id': row[1], 'nome': row[2]} for row in cur.fetchall()]
    cur.execute('SELECT pk, organization, cnpj FROM organizations_app WHERE active = true ORDER BY organization')
    empresas = [{'pk': row[0], 'organization': row[1], 'cnpj': row[2]} for row in cur.fetchall()]
    associacoes = []
    for usuario in usuarios:
        cur.execute('''
            SELECT o.pk, o.organization 
            FROM user_organizations ua 
            JOIN organizations_app o ON o.pk = ua.organization_id
            WHERE ua.user_id = %s
            ORDER BY o.organization
        ''', (usuario['id'],))
        empresas_associadas = [{'pk': row[0], 'organization': row[1]} for row in cur.fetchall()]
        if empresas_associadas:
            associacoes.append({
                'usuario': usuario,
                'empresas': empresas_associadas
            })
    cur.close()
    conn.close()
    return render_template('associacoes.html', usuarios=usuarios, empresas=empresas, associacoes=associacoes)

@app.route('/configuracoes/associacoes/buscar/<int:user_id>')
@login_required
@admin_required
def buscar_associacoes(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT organization_id FROM user_organizations WHERE user_id = %s', (user_id,))
    empresas = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify({'empresas': empresas})

@app.route('/configuracoes/associacoes/editar/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def editar_associacoes(user_id):
    empresas = request.form.getlist('empresas[]')
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM user_organizations WHERE user_id = %s', (user_id,))
        for empresa_id in empresas:
            cur.execute('INSERT INTO user_organizations (user_id, organization_id) VALUES (%s, %s)',
                        (user_id, empresa_id))
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()
        conn.close()

@app.route('/configuracoes/associacoes/excluir/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
def excluir_associacoes(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM user_organizations WHERE user_id = %s', (user_id,))
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()
        conn.close()

@app.route('/download-imagens/<int:study_pk>')
@login_required
def download_imagens(study_pk):
    formato = request.args.get('formato', 'jpg')
    
    if formato not in ['jpg', 'dicom']:
        return jsonify({'error': 'Formato inválido'}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Buscar informações do estudo
        cur.execute("""
            SELECT s.study_iuid, p.pat_name, s.study_datetime
            FROM study s
            JOIN patient p ON p.pk = s.patient_fk
            WHERE s.pk = %s
        """, (study_pk,))
        
        study_info = cur.fetchone()
        if not study_info:
            return jsonify({'error': 'Estudo não encontrado'}), 404
        
        study_iuid, pat_name, study_datetime = study_info
        
        # Query para buscar o diretório do estudo
        archive_query = """
            SELECT f.dirpath
            FROM files fl
            JOIN instance ins ON fl.instance_fk = ins.pk
            JOIN series sr ON ins.series_fk = sr.pk
            JOIN study st ON sr.study_fk = st.pk
            JOIN filesystem f ON f.pk = fl.filesystem_fk
            WHERE st.pk = %s AND sr.modality != 'SR'
            LIMIT 1
        """
        cur.execute(archive_query, [study_pk])
        archive_result = cur.fetchone()
        
        if archive_result and archive_result[0]:
            # Extrair apenas o último diretório do caminho
            dirpath_parts = archive_result[0].split('\\')
            archive_path = dirpath_parts[-1] if dirpath_parts else "archive"
        else:
            archive_path = "archive"
        
        # Buscar todas as imagens do estudo
        cur.execute("""
            SELECT concat(fl.filepath) AS caminho, ins.sop_iuid
            FROM files fl
            JOIN instance ins ON fl.instance_fk = ins.pk
            JOIN series sr ON ins.series_fk = sr.pk
            JOIN study st ON sr.study_fk = st.pk
            WHERE st.pk = %s AND sr.modality != 'SR'
            ORDER BY fl.created_time ASC
        """, (study_pk,))
        
        images = cur.fetchall()
        
        if not images:
            return jsonify({'error': 'Nenhuma imagem encontrada para este estudo'}), 404
        
        # Criar diretório temporário
        temp_dir = tempfile.mkdtemp()
        zip_filename = f"{pat_name}_{study_datetime.strftime('%Y%m%d')}_{formato}.zip"
        zip_path = os.path.join(temp_dir, zip_filename)
        
        dicom_base_url = f"http://{SERVER_IP}/{archive_path}/"
        print(f"DEBUG: Base URL: {dicom_base_url}")
        print(f"DEBUG: Total de imagens encontradas: {len(images)}")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for idx, (filepath, sop_iuid) in enumerate(images):
                # Construir URL do arquivo DICOM
                dicom_url = f"{dicom_base_url}{filepath}"
                print(f"DEBUG: Tentando baixar: {dicom_url}")
                
                try:
                    # Baixar arquivo DICOM via HTTP com autenticação básica
                    auth = (f'{NGINX_AUTH_USER}', f'{NGINX_AUTH_PASSWORD}')  # Credenciais diretas para o Nginx
                    response = requests.get(dicom_url, auth=auth, timeout=10)
                    print(f"DEBUG: Status da resposta: {response.status_code}")
                    if response.status_code == 200:
                        print(f"DEBUG: Arquivo baixado com sucesso, tamanho: {len(response.content)} bytes")
                        if formato == 'dicom':
                            # Adicionar arquivo DICOM diretamente
                            arcname = f"image_{idx+1:04d}.dcm"
                            zipf.writestr(arcname, response.content)
                            print(f"DEBUG: Arquivo DICOM adicionado ao ZIP: {arcname}")
                        else:
                            # Converter DICOM para JPG
                            try:
                                ds = pydicom.dcmread(BytesIO(response.content))
                                if hasattr(ds, 'pixel_array'):
                                    pixel_array = ds.pixel_array
                                    
                                    # Normalizar para 0-255
                                    if pixel_array.max() > 255:
                                        pixel_array = (pixel_array / pixel_array.max() * 255).astype('uint8')
                                    
                                    # Converter para PIL Image
                                    if len(pixel_array.shape) == 2:
                                        image = Image.fromarray(pixel_array, mode='L')
                                    else:
                                        image = Image.fromarray(pixel_array)
                                    
                                    # Salvar como JPG em memória
                                    img_buffer = BytesIO()
                                    image.save(img_buffer, format='JPEG', quality=95)
                                    img_buffer.seek(0)
                                    
                                    # Adicionar ao ZIP
                                    arcname = f"image_{idx+1:04d}.jpg"
                                    zipf.writestr(arcname, img_buffer.getvalue())
                                    print(f"DEBUG: Arquivo JPG adicionado ao ZIP: {arcname}")
                            except Exception as e:
                                print(f"Erro ao processar imagem {sop_iuid}: {e}")
                                continue
                    else:
                        print(f"DEBUG: Falha ao baixar arquivo, status: {response.status_code}")
                except Exception as e:
                     print(f"Erro ao baixar imagem {filepath}: {e}")
                     continue
        
        print(f"DEBUG: Arquivo ZIP criado: {zip_path}")
        print(f"DEBUG: Tamanho do arquivo ZIP: {os.path.getsize(zip_path)} bytes")
        
        # Fechar conexão antes do envio
        cur.close()
        conn.close()
        
        # Função para limpar diretório temporário após o download
        def remove_temp_dir():
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
        
        # Enviar arquivo ZIP com callback para limpeza
        response = send_file(
            zip_path,
            as_attachment=True,
            download_name=zip_filename,
            mimetype='application/zip'
        )
        
        # Agendar limpeza do diretório temporário
        import atexit
        atexit.register(remove_temp_dir)
        
        return response
        
    except Exception as e:
        cur.close()
        conn.close()
        return jsonify({'error': f'Erro interno: {str(e)}'}), 500