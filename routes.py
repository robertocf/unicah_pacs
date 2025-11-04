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
import uuid
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
from services.disk_reaming import get_free_space_bytes, get_average_daily_usage_bytes, REPOSITORY_PATH, DAYS_TO_AVERAGE, format_bytes
from services.storage_stats import get_storage_stats
from services.audit_logs import insert_log_registro, _get_existing_patient_data, insert_login_log
from services.permissions import get_user_permissions, list_permission_definitions

@app.context_processor
def inject_permissions():
    try:
        if current_user and getattr(current_user, 'is_authenticated', False):
            return {'permissions': get_user_permissions(current_user)}
        return {'permissions': {}}
    except Exception:
        return {'permissions': {}}

@app.context_processor
def inject_app_version():
    # Disponibiliza APP_VERSION em todos os templates
    try:
        return {'APP_VERSION': app.config.get('APP_VERSION', '1.0.0')}
    except Exception:
        return {'APP_VERSION': '1.0.0'}


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

    # Paginação
    try:
        page = int(request.args.get('page', 1))
    except (TypeError, ValueError):
        page = 1
    per_page = 10
    total_items = len(pacientes)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    pacientes_page = pacientes[start:end]

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
        pacientes=pacientes_page,
        page=page,
        per_page=per_page,
        total_items=total_items,
        total_pages=total_pages
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

# --- Configuração de importação DICOM para PACS ---
IMPORT_UPLOAD_DIR = os.path.join(tempfile.gettempdir(), 'dicom_imports')
os.makedirs(IMPORT_UPLOAD_DIR, exist_ok=True)

# Valores padrão para destino PACS (ajustáveis na UI)
DEFAULT_PACS_HOST = SERVER_IP
DEFAULT_PACS_PORT = 104
DEFAULT_PACS_AET = 'ORTHANC'
DEFAULT_LOCAL_AET = 'IMPORT-SCU'

# Armazenamento em memória de sessões de upload
IMPORT_SESSIONS = {}
# --------------------------------------------------

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
        return redirect(url_for("home"))
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
            insert_login_log(usuario_nome=(user.name or user.user_id))
            return redirect(url_for("home"))
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

@app.route("/home", methods=["GET"]) 
@login_required
def home():
    return render_template("homepage.html")

@app.route("/estudos", methods=["GET", "POST"]) 
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

@app.route('/dicom/importar', methods=['GET'])
@login_required
@admin_required
def importar_dicom_page():
    return render_template(
        'importar_dicom.html',
        default_pacs_host=DEFAULT_PACS_HOST,
        default_pacs_port=DEFAULT_PACS_PORT,
        default_pacs_aet=DEFAULT_PACS_AET,
        default_local_aet=DEFAULT_LOCAL_AET,
    )

@app.route('/dicom/importar/preview', methods=['POST'])
@login_required
@admin_required
def importar_dicom_preview():
    try:
        files = request.files.getlist('files')
        if not files:
            return jsonify({'message': 'Nenhum arquivo enviado'}), 400
        batch_id = uuid.uuid4().hex
        batch_dir = os.path.join(IMPORT_UPLOAD_DIR, batch_id)
        os.makedirs(batch_dir, exist_ok=True)

        groups = {}
        idx = 0
        for f in files:
            if not f or f.filename == '':
                continue
            filename = secure_filename(f.filename)
            save_path = os.path.join(batch_dir, f"{idx:06d}_{filename}")
            f.save(save_path)
            idx += 1
            try:
                ds = pydicom.dcmread(save_path, stop_before_pixels=True, force=True)
            except Exception:
                ds = None

            patient_id = str(getattr(ds, 'PatientID', '') or '') if ds else ''
            patient_name = str(getattr(ds, 'PatientName', '') or '') if ds else ''
            study_date = str(getattr(ds, 'StudyDate', '') or '') if ds else ''
            modality = str(getattr(ds, 'Modality', '') or '') if ds else ''
            study_desc = str(getattr(ds, 'StudyDescription', '') or '') if ds else ''
            accession_number = str(getattr(ds, 'AccessionNumber', '') or '') if ds else ''
            study_uid = str(getattr(ds, 'StudyInstanceUID', '') or '') if ds else ''

            # Definir chave de agrupamento: por estudo/procedimento
            if study_uid:
                key = ('STUDY', study_uid)
            elif accession_number:
                key = ('ACC', patient_id, accession_number)
            else:
                key = ('FALLBACK', patient_id, study_date, modality or '', study_desc or '')

            g = groups.get(key)
            if not g:
                g = {
                    'file_paths': [],
                    'patient_id': patient_id,
                    'patient_name': patient_name,
                    'study_date': None,
                    'modality_set': set(),
                    'study_desc': None,
                    'accession_number': None,
                    'study_uid': None,
                }
                groups[key] = g
            g['file_paths'].append(save_path)
            if study_date:
                g['study_date'] = g['study_date'] or study_date
            if modality:
                g['modality_set'].add(modality)
            if study_desc and not g['study_desc']:
                g['study_desc'] = study_desc
            if accession_number and not g['accession_number']:
                g['accession_number'] = accession_number
            if study_uid and not g['study_uid']:
                g['study_uid'] = study_uid
            if patient_name and not g['patient_name']:
                g['patient_name'] = patient_name
            if patient_id and not g['patient_id']:
                g['patient_id'] = patient_id

        # Construir itens agregados
        items = []
        groups_list = []
        for _, g in groups.items():
            modalities = sorted(list(g['modality_set'])) if g['modality_set'] else []
            modality_text = '/'.join(modalities) if modalities else ''
            items.append({
                'patient_id': g['patient_id'] or '',
                'patient_name': g['patient_name'] or '',
                'study_date': g['study_date'] or '',
                'modality': modality_text,
                'study_desc': g['study_desc'] or '',
                'accession_number': g['accession_number'] or '',
                'files_count': len(g['file_paths']) if g.get('file_paths') else 0,
                'count': len(g['file_paths']) if g.get('file_paths') else 0,
            })
            groups_list.append({'file_paths': g['file_paths']})

        IMPORT_SESSIONS[batch_id] = {
            'dir': batch_dir,
            'files': [],
            'groups': groups_list,
        }
        return jsonify({'batch_id': batch_id, 'items': items})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.route('/dicom/importar/enviar', methods=['POST'])
@login_required
@admin_required
def importar_dicom_enviar():
    data = request.get_json(silent=True) or {}
    batch_id = data.get('batch_id')
    selected_indices = data.get('selected_indices') or []
    dest_host = data.get('dest_host') or DEFAULT_PACS_HOST
    dest_port = int(data.get('dest_port') or DEFAULT_PACS_PORT)
    dest_aet = data.get('dest_aet') or DEFAULT_PACS_AET
    local_aet = data.get('local_aet') or DEFAULT_LOCAL_AET

    if not batch_id or batch_id not in IMPORT_SESSIONS:
        return jsonify({'message': 'Lote inválido ou expirado'}), 400
    session = IMPORT_SESSIONS[batch_id]
    groups = session.get('groups', [])
    if not groups:
        return jsonify({'message': 'Nenhum grupo disponível para envio'}), 400

    if not selected_indices:
        selected_indices = list(range(len(groups)))

    ae = AE(ae_title=local_aet)
    ae.requested_contexts = AllStoragePresentationContexts

    assoc = ae.associate(dest_host, dest_port, ae_title=dest_aet)
    if not assoc.is_established:
        return jsonify({'message': 'Falha ao conectar ao PACS (Association não estabelecida)'}), 502

    sent_ok = 0
    sent_fail = 0
    for i in selected_indices:
        if i < 0 or i >= len(groups):
            sent_fail += 1
            continue
        file_paths = groups[i].get('file_paths', [])
        for path in file_paths:
            try:
                ds = pydicom.dcmread(path, force=True)
                status = assoc.send_c_store(ds)
                if status and status.Status in (0x0000,):
                    sent_ok += 1
                else:
                    sent_fail += 1
            except Exception:
                sent_fail += 1

    assoc.release()

    return jsonify({'sent_ok': sent_ok, 'sent_fail': sent_fail})

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

    # Buscar contexto (empresa_id, modalidade, data_estudo) ANTES da deleção
    ctx_empresa_id = None
    ctx_modalidade = None
    ctx_data_estudo = None
    try:
        conn_ctx = get_db_connection()
        cur_ctx = conn_ctx.cursor()
        cur_ctx.execute(
            """
            SELECT 
                oa.pk  AS empresa_id,
                sr.modality AS modalidade_estudo,
                s.study_datetime AS data_estudo
            FROM patient p
            JOIN study s ON s.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = s.pk
            LEFT JOIN organizations_app oa ON oa.presentation = sr.institution
            WHERE p.pat_id = %s
            ORDER BY s.study_datetime DESC NULLS LAST
            LIMIT 1
            """,
            (pat_id,),
        )
        row = cur_ctx.fetchone()
        if row:
            ctx_empresa_id = row[0]
            ctx_modalidade = row[1]
            ctx_data_estudo = row[2]
    except Exception:
        pass
    finally:
        try:
            cur_ctx.close()
            conn_ctx.close()
        except Exception:
            pass

    if pat_sex == "":
        pat_sex = '""'

    hl7_msg = f"""MSH|^~\\&|SISTEMA_ORIGEM|CLINICA ARTUS|DCM4CHEE|DCM4CHEE|{datetime.now().strftime('%Y%m%d%H%M%S')}||ADT^A23|MSG_{pat_id}|P|2.3
EVN|A23|{datetime.now().strftime('%Y%m%d%H%M%S')}
PID|1||{pat_id}^^^||{pat_name}^^^||{pat_birthdate.replace('-','')}|{pat_sex}||"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((f"{SERVER_IP}", 6663))
            s.sendall(b"\x0b" + hl7_msg.encode() + b"\x1c\x0d")
        # Inserir log de deleção com contexto capturado
        insert_log_registro(
            tipo_acao="DELETE",
            paciente_id=pat_id,
            nome_paciente=pat_name,
            usuario_nome=current_user.name,
            paciente_birthdate=pat_birthdate or None,
            paciente_sex=(None if pat_sex in ("", '""') else pat_sex),
            empresa_id=ctx_empresa_id,
            modalidade_estudo=ctx_modalidade,
            data_estudo=ctx_data_estudo,
        )
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
        top_margin = 800
        images_per_page = 1
        rows = 1
        cols = 1
        row_spacing = 2
        img_height = 700
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
    # Snapshot dos dados antes da atualização para comparar corretamente
    snapshot_antes = _get_existing_patient_data(pat_id)
    hl7_msg = f"""MSH|^~\\&|SISTEMA_ORIGEM|CLINICA ARTUS|DCM4CHEE|DCM4CHEE|{datetime.now().strftime('%Y%m%d%H%M%S')}||ADT^A08|MSG_{pat_id}|P|2.3
EVN|A08|{datetime.now().strftime('%Y%m%d%H%M%S')}
PID|1||{pat_id}^^^||{pat_name}^^^||{pat_birthdate.replace('-','')}|{pat_sex}||"""
    print(hl7_msg)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((f"{SERVER_IP}", 6662))
            s.sendall(b"\x0b" + hl7_msg.encode() + b"\x1c\x0d")
        # Inserir log de atualização
        insert_log_registro(
            tipo_acao="UPDATE",
            paciente_id=pat_id,
            nome_paciente=pat_name,
            usuario_nome=current_user.name,
            paciente_birthdate=pat_birthdate or None,
            paciente_sex=(None if pat_sex in ("", '""') else pat_sex),
            dados_atuais_anterior=snapshot_antes,
        )
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
            ds = pydicom.dcmread(BytesIO(response.content), force=True)
            pixel_array = getattr(ds, 'pixel_array', None)
            if pixel_array is None:
                return "DICOM sem imagem", 400
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
WITH fs_base AS (
    SELECT 
        f2.pk,
        CASE 
            WHEN f2.dirpath = 'archive'
            THEN 'C:\\server\\dcm4chee-2.17.3-psql\\server\\default\\archive'
            ELSE f2.dirpath
        END AS dirpath,
        f2.retrieve_aet,
        f2.fs_status,
        f2.fs_group_id
    FROM filesystem f2
)
SELECT 
    fb.pk,
    fb.dirpath,
    fb.fs_group_id,
    fb.retrieve_aet,
    fb.fs_status,
    dm.total,
    dm.used,
    ROUND(COALESCE(SUM(f.file_size), 0) / 1024 / 1024, 2) AS qtd
FROM fs_base fb
LEFT JOIN files f ON f.filesystem_fk = fb.pk
JOIN disk_monitor dm
    ON dm.drive = CONCAT(SPLIT_PART(fb.dirpath, E'\\\\', 1), E'\\\\')
GROUP BY 
    fb.pk,
    fb.dirpath,
    fb.fs_group_id,
    fb.fs_status,
    fb.retrieve_aet,
    dm.total,
    dm.used
ORDER BY fb.fs_status
    """)
    directories = [{
        'pk': row[0],
        'dirpath': row[1],
        'fs_group_id': row[2],
        'retrieve_aet': row[3],
        'fs_status': row[4],
        'total': row[5],
        'used': row[6],
        'qtd': row[7]
    } for row in cur.fetchall()]
    cur.close()
    conn.close()

    # Métricas gerais de armazenamento
    storage_stats = get_storage_stats()

    # Soma total de capacidade de todos os repositórios (disk_monitor)
    conn_dm = get_db_connection()
    cur_dm = conn_dm.cursor()
    try:
        cur_dm.execute("SELECT COALESCE(SUM(total), 0) FROM disk_monitor")
        total_bytes_all = cur_dm.fetchone()[0] or 0
    finally:
        cur_dm.close()
        conn_dm.close()
    overall_total_gb = round(float(total_bytes_all) / 1024 / 1024 / 1024, 2)

    # Agregação mensal (últimos 12 meses) em GB para fs_status = 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            WITH monthly AS (
                SELECT DATE_TRUNC('month', s.created_time) AS month_start,
                       SUM(f.file_size) AS total_bytes
                FROM study s
                JOIN series se ON s.pk = se.study_fk
                JOIN instance i ON se.pk = i.series_fk
                JOIN files f ON i.pk = f.instance_fk
                JOIN filesystem fs ON f.filesystem_fk = fs.pk
                WHERE fs.fs_status = 0
                  AND s.created_time IS NOT NULL
                  AND se.modality != 'SR'
                  AND s.created_time >= NOW() - INTERVAL '12 months'
                GROUP BY month_start
            )
            SELECT TO_CHAR(month_start, 'MM/YYYY') AS mes_label,
                   ROUND(COALESCE(total_bytes, 0) / 1024 / 1024 / 1024, 2) AS gb
            FROM monthly
            ORDER BY month_start
        """)
        rows = cur.fetchall()
        monthly_labels = [r[0] for r in rows]
        monthly_gb = [float(r[1]) for r in rows]
    finally:
        cur.close()
        conn.close()

    return render_template('armazenamento.html', directories=directories, storage_stats=storage_stats, monthly_labels=monthly_labels, monthly_gb=monthly_gb, overall_total_gb=overall_total_gb)

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
        
        # Nome e data seguros para arquivo
        safe_name = secure_filename(pat_name) or "Paciente"
        date_str = study_datetime.strftime('%Y%m%d') if hasattr(study_datetime, 'strftime') else datetime.now().strftime('%Y%m%d')
        
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
        zip_filename = f"{safe_name}_{date_str}_{formato}.zip"
        zip_path = os.path.join(temp_dir, zip_filename)
        
        dicom_base_url = f"http://{SERVER_IP}/{archive_path}/"
        print(f"DEBUG: Base URL: {dicom_base_url}")
        print(f"DEBUG: Total de imagens encontradas: {len(images)}")
        
        files_added = 0
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
                            files_added += 1
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
                                    files_added += 1
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
        
        # Se nenhum arquivo foi adicionado, retornar erro amigável
        if files_added == 0:
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
            return jsonify({'error': 'Não foi possível anexar imagens ao ZIP. Verifique credenciais ou servidor.'}), 502
        
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


# Rotas do módulo Gerencial
@app.route('/gerencial', methods=['GET'])
@login_required
@admin_required
def gerencial():
    """Página principal do módulo gerencial com logs de eventos"""
    return render_template('gerencial.html', logs=None)


@app.route('/gerencial/search', methods=['GET', 'POST'])
@login_required
@admin_required
def gerencial_search():
    """Rota para pesquisar logs de eventos na tabela log_registros"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Obter parâmetros (funciona com GET e POST)
        data_inicio = request.values.get('data_inicio')
        data_fim = request.values.get('data_fim')
        paciente_id = request.values.get('paciente_id')
        nome_paciente = request.values.get('nome_paciente')
        tipo_acao = request.values.get('tipo_acao')
        empresa_id = request.values.get('empresa_id')
        usuario_id = request.values.get('usuario_id')
        
        # Parâmetros de paginação
        page = int(request.args.get('page', 1))
        per_page = 10
        
        # Construir query SQL dinamicamente para contar total
        count_query = """
            SELECT COUNT(*) 
            FROM log_registros 
            WHERE 1=1
        """
        params = []
        
        # Adicionar filtros conforme os parâmetros fornecidos
        if data_inicio:
            count_query += " AND DATE(data_hora) >= %s"
            params.append(data_inicio)
            
        if data_fim:
            count_query += " AND DATE(data_hora) <= %s"
            params.append(data_fim)
            
        if paciente_id:
            count_query += " AND paciente_id LIKE %s"
            params.append(f"%{paciente_id}%")
            
        if nome_paciente:
            count_query += " AND nome_paciente LIKE %s"
            params.append(f"%{nome_paciente}%")
            
        if tipo_acao:
            if tipo_acao.upper() == 'LOGIN':
                # Buscar tanto registros com tipo_acao=LOGIN quanto os gravados como INSERT com contexto de LOGIN
                count_query += " AND (tipo_acao = 'LOGIN' OR (tipo_acao = 'INSERT' AND contexto ILIKE %s))"
                params.append('%LOGIN%')
            else:
                count_query += " AND tipo_acao = %s"
                params.append(tipo_acao)
            
        if empresa_id:
            count_query += " AND empresa_id LIKE %s"
            params.append(f"%{empresa_id}%")
            
        if usuario_id:
            count_query += " AND usuario_id LIKE %s"
            params.append(f"%{usuario_id}%")
        
        # Executar query de contagem
        cur.execute(count_query, params)
        total_items = cur.fetchone()[0]
        total_pages = (total_items + per_page - 1) // per_page
        
        # Construir query principal com paginação
        query = """
            SELECT data_hora, empresa_id, usuario_id, tipo_acao, paciente_id, 
                   nome_paciente, modalidade_estudo, data_estudo, contexto 
            FROM log_registros 
            WHERE 1=1
        """
        
        # Adicionar os mesmos filtros
        if data_inicio:
            query += " AND DATE(data_hora) >= %s"
            
        if data_fim:
            query += " AND DATE(data_hora) <= %s"
            
        if paciente_id:
            query += " AND paciente_id LIKE %s"
            
        if nome_paciente:
            query += " AND nome_paciente LIKE %s"
            
        if tipo_acao:
            if tipo_acao.upper() == 'LOGIN':
                query += " AND (tipo_acao = 'LOGIN' OR (tipo_acao = 'INSERT' AND contexto ILIKE %s))"
            else:
                query += " AND tipo_acao = %s"
            
        if empresa_id:
            query += " AND empresa_id LIKE %s"
            
        if usuario_id:
            query += " AND usuario_id LIKE %s"
        
        # Ordenar por data mais recente primeiro e aplicar paginação
        offset = (page - 1) * per_page
        query += f" ORDER BY data_hora DESC LIMIT {per_page} OFFSET {offset}"
        
        # Executar query principal
        cur.execute(query, params)
        results = cur.fetchall()
        
        # Converter resultados para objetos com atributos nomeados
        logs = []
        for row in results:
            log = type('Log', (), {
                'data_hora': row[0],
                'empresa_id': row[1],
                'usuario_id': row[2],
                'tipo_acao': row[3],
                'paciente_id': row[4],
                'nome_paciente': row[5],
                'modalidade_estudo': row[6],
                'data_estudo': row[7],
                'contexto': row[8]
            })()
            logs.append(log)
        
        cur.close()
        conn.close()
        
        return render_template('gerencial.html', 
                             logs=logs,
                             page=page,
                             per_page=per_page,
                             total_items=total_items,
                             total_pages=total_pages,
                             data_inicio=data_inicio,
                             data_fim=data_fim,
                             paciente_id=paciente_id,
                             nome_paciente=nome_paciente,
                             tipo_acao=tipo_acao,
                             empresa_id=empresa_id,
                             usuario_id=usuario_id)
        
    except Exception as e:
        print(f"Erro ao pesquisar logs: {e}")
        flash(f'Erro ao pesquisar logs: {str(e)}', 'error')
        return render_template('gerencial.html', logs=None)

# Página de Permissões (layout, sem persistência)
@app.route('/configuracoes/permissoes', methods=['GET'])
@login_required
@admin_required
def configuracoes_permissoes():
    usuarios = User.query.all()
    roles = sorted({u.role for u in usuarios if getattr(u, 'role', None)})
    return render_template('permissoes.html', usuarios=usuarios, roles=roles, permission_defs=list_permission_definitions())