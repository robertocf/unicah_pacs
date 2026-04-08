from flask import request, render_template
from datetime import datetime
from config import SERVER_IP  # ajuste se o import do config for diferente
from db import get_db_connection  # ajuste se o import da conexão for diferente

def carregar_homepage(user_name, user_id, user_role=None, alert=None, permissions=None):  
    if permissions is None:
        permissions = {}
    if request.method == "POST":
        id_paciente = request.form.get("id_paciente", "")
        nome = request.form.get("nome", "")
        data_nascimento = request.form.get("data_nascimento", "")
        sexo = request.form.get("sexo", "")
        data_atendimento = request.form.get("data_atendimento", "all")
        modalidade = request.form.get("modalidade", "all")
        procedimento = request.form.get("procedimento", "")
        protocolo = request.form.get("protocolo", "")
        status = request.form.get("status", "")
        qtd_operador = request.form.get("qtd_operador", "")
        qtd_valor = request.form.get("qtd_valor", "")
        per_page_param = request.form.get("per_page", 10)
        sort_by = request.form.get("sort_by", "data")
        sort_order = request.form.get("sort_order", "desc")
    else:
        id_paciente = request.args.get("id_paciente", "")
        nome = request.args.get("nome", "")
        data_nascimento = request.args.get("data_nascimento", "")
        sexo = request.args.get("sexo", "")
        data_atendimento = request.args.get("data_atendimento", "all")
        modalidade = request.args.get("modalidade", "all")
        procedimento = request.args.get("procedimento", "")
        protocolo = request.args.get("protocolo", "")
        status = request.args.get("status", "")
        qtd_operador = request.args.get("qtd_operador", "")
        qtd_valor = request.args.get("qtd_valor", "")
        per_page_param = request.args.get("per_page", 10)
        sort_by = request.args.get("sort_by", "data")
        sort_order = request.args.get("sort_order", "desc")

    # Validar sort_order
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'

    # Mapeamento de colunas para ordenação
    sort_mapping = {
        'id': 'p.pat_id',
        'nome': 'pat_name',
        'nascimento': 'p.pat_birthdate',
        'idade': 'p.pat_birthdate', 
        'sexo': 'p.pat_sex',
        'modalidade': 'sr.modality',
        'procedimento': 'study_desc',
        'protocolo': 's.study_id',
        'data': 's.study_datetime',
        'status': 'custom',
        'qtd': 's.num_instances'
    }
    
    # Se a coluna não estiver no mapa, usa data como padrão
    order_column = sort_mapping.get(sort_by, 's.study_datetime')
    
    # Converter per_page_param para int se for numérico para coincidir com as opções do template
    if per_page_param != "Todas":
        try:
            per_page_param = int(per_page_param)
        except (ValueError, TypeError):
            per_page_param = 10

    # Se for POST (nova pesquisa ou alteração de per_page), resetar para página 1
    if request.method == "POST":
        page = 1
    else:
        page = int(request.args.get("page", 1))
    
    per_page_options = [10, 15, 50, "Todas"]
    
    if per_page_param == "Todas":
        per_page = None  # Sem limite
        offset = 0
    else:
        per_page = int(per_page_param)
        if per_page not in [10, 15, 50]:
            per_page = 10
        offset = (page - 1) * per_page

    # Parte base da query
    if user_role == 'admin':
        base_query = """
            SELECT p.pat_id,
                   split_part(p.pat_name, '^', 1) AS pat_name,
                   CASE 
                    WHEN LENGTH(p.pat_birthdate) = 8 
                        AND p.pat_birthdate ~ '^[0-9]{8}$' 
                    THEN to_char(to_date(p.pat_birthdate, 'YYYYMMDD'), 'DD/MM/YYYY')
                    ELSE ''
                    END as pat_birthdate,
                   CASE 
                    WHEN LENGTH(p.pat_birthdate) = 8 AND p.pat_birthdate ~ '^[0-9]{8}$'
                    THEN EXTRACT(YEAR FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || 'a e ' ||
                         EXTRACT(MONTH FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || 'm'
                    ELSE '' 
                   END AS idade,
                   CASE WHEN p.pat_sex IS NULL THEN '' ELSE p.pat_sex END AS pat_sex,
                   sr.modality,
                   CASE
                    WHEN s.study_desc IS NULL THEN '' else s.study_desc end as study_desc,
                   s.pk,
                   to_char(s.study_datetime, 'DD/MM/YYYY HH24:MI:SS') as study_datetime,
                   CASE s.study_custom1 WHEN 'I' THEN 'Impresso' WHEN 'V' THEN 'Visual' WHEN 'R' THEN 'Rascu' WHEN 'A' THEN 'Assinado' ELSE 'Pronto' END AS custom,
                   s.num_instances,
                   s.pk,
                   CASE WHEN sr.institution IS NULL THEN '' else sr.institution END AS institution, 
                   CASE WHEN  sr.station_name IS NULL THEN '' else  sr.station_name END AS  station_name,
                   CASE WHEN s.ref_physician IS NULL THEN '' else s.ref_physician END AS ref_physician,
                   CASE WHEN s.study_id IS NULL THEN '' else s.study_id END AS study_id,
                   s.study_iuid,
                   CASE WHEN s.accession_no IS NULL THEN '' else s.accession_no END AS accession_no,
                   u.name AS doctor_assigned
            FROM patient p
            JOIN study s ON s.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = s.pk
            LEFT JOIN users_app u ON s.doctor_id = u.pk
            WHERE sr.modality != 'SR'
        """
    else:
        base_query = """
            SELECT p.pat_id,
                   split_part(p.pat_name, '^', 1) AS pat_name,
                   CASE 
                    WHEN LENGTH(p.pat_birthdate) = 8 
                        AND p.pat_birthdate ~ '^[0-9]{8}$' 
                    THEN to_char(to_date(p.pat_birthdate, 'YYYYMMDD'), 'DD/MM/YYYY')
                    ELSE ''
                    END as pat_birthdate,
                   CASE 
                    WHEN LENGTH(p.pat_birthdate) = 8 AND p.pat_birthdate ~ '^[0-9]{8}$'
                    THEN EXTRACT(YEAR FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' anos e ' ||
                         EXTRACT(MONTH FROM AGE(TO_DATE(pat_birthdate, 'YYYYMMDD'))) || ' meses'
                    ELSE '' 
                   END AS idade,
                   CASE WHEN p.pat_sex IS NULL THEN '' ELSE p.pat_sex END AS pat_sex,
                   sr.modality,
                   CASE
                    WHEN s.study_desc IS NULL THEN '' else s.study_desc end as study_desc,
                   s.pk,
                   to_char(s.study_datetime, 'DD/MM/YYYY HH24:MI:SS') as study_datetime,
                   CASE s.study_custom1 WHEN 'I' THEN 'Impresso' WHEN 'V' THEN 'Visual' WHEN 'R' THEN 'Rascu' WHEN 'A' THEN 'Assinado' ELSE 'Pronto' END AS custom,
                   s.num_instances,
                   s.pk,
                   CASE WHEN sr.institution IS NULL THEN '' else sr.institution END AS institution, 
                   CASE WHEN  sr.station_name IS NULL THEN '' else  sr.station_name END AS  station_name,
                   CASE WHEN s.ref_physician IS NULL THEN '' else s.ref_physician END AS ref_physician,
                   CASE WHEN s.study_id IS NULL THEN '' else s.study_id END AS study_id,
                   s.study_iuid,
                   CASE WHEN s.accession_no IS NULL THEN '' else s.accession_no END AS accession_no,
                   u.name AS doctor_assigned
            FROM patient p
            JOIN study s ON s.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = s.pk
            LEFT JOIN users_app u ON s.doctor_id = u.pk
            WHERE sr.modality != 'SR'
            AND sr.institution IN (
                SELECT oa.presentation 
                FROM organizations_app oa, user_organizations uo 
                WHERE oa.pk = uo.organization_id AND uo.user_id = %s
            )
        """

    group_by_clause = """
        GROUP BY 
            p.pat_id,
            split_part(p.pat_name, '^', 1),
            p.pat_birthdate,
            p.pat_sex,
            sr.modality,
            s.study_desc,
            sr.institution, 
            s.pk,
            sr.station_name,
            s.ref_physician,
            s.study_id,
            s.study_iuid,
            s.num_instances,
            s.accession_no,
            u.name
    """

    # Condições dinâmicas
    params = [user_id] if user_role != 'admin' else []
    conditions = []

    # Se não tiver permissão para ver todos, filtra pelo ID do médico
    if not permissions.get('ver_todos_estudos') and user_role != 'admin':
        conditions.append("s.doctor_id = %s")
        params.append(user_id)

    if id_paciente and id_paciente.isdigit():
        conditions.append("p.pat_id = %s")
        params.append(id_paciente)
    if nome:
        conditions.append("split_part(p.pat_name, '^', 1) ILIKE %s")
        params.append(f"%{nome}%")
    if data_nascimento:
        try:
            datetime.strptime(data_nascimento, "%Y-%m-%d")
            conditions.append("p.pat_birthdate = %s")
            params.append(data_nascimento)
        except ValueError:
            pass
    if sexo:
        conditions.append("p.pat_sex = %s")
        params.append(sexo)
    if data_atendimento and data_atendimento != "all":
        if data_atendimento == "today":
            conditions.append("s.study_datetime::date = CURRENT_DATE")
        elif data_atendimento == "last3days":
            conditions.append("s.study_datetime::date >= CURRENT_DATE - INTERVAL '3 days'")
        elif data_atendimento == "last30days":
            conditions.append("s.study_datetime::date >= CURRENT_DATE - INTERVAL '30 days'")
        else:
            try:
                datetime.strptime(data_atendimento, "%Y-%m-%d")
                conditions.append("s.study_datetime::date = %s")
                params.append(data_atendimento)
            except ValueError:
                pass

    if modalidade != "all":
        conditions.append("sr.modality = %s")
        params.append(modalidade)

    if procedimento:
        conditions.append("s.study_desc ILIKE %s")
        params.append(f"%{procedimento}%")

    if protocolo:
        conditions.append("s.study_id ILIKE %s")
        params.append(f"%{protocolo}%")

    if status:
        if status == 'Impresso':
            conditions.append("s.study_custom1 = 'I'")
        elif status == 'Visual':
            conditions.append("s.study_custom1 = 'V'")
        elif status == 'Rascu':
            conditions.append("s.study_custom1 = 'R'")
        elif status == 'Assinado':
            conditions.append("s.study_custom1 = 'A'")
        elif status == 'Pronto':
             conditions.append("(s.study_custom1 IS NULL OR s.study_custom1 NOT IN ('I', 'V', 'R', 'A'))")

    if qtd_valor and qtd_valor.isdigit() and qtd_operador in ['>', '<', '=', '>=', '<=']:
         conditions.append(f"s.num_instances {qtd_operador} %s")
         params.append(qtd_valor)

    # Monta a query completa
    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    full_query = base_query + group_by_clause

    count_query = f"SELECT COUNT(*) FROM ({full_query}) AS subquery"
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(count_query, params)
    total_records = cur.fetchone()[0]
    
    if per_page is None:
        # Mostrar todas as páginas
        total_pages = 1
        full_query += f"""
            ORDER BY {order_column} {sort_order.upper()}
        """
    else:
        total_pages = (total_records + per_page - 1) // per_page
        full_query += f"""
            ORDER BY {order_column} {sort_order.upper()}
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, offset])
    cur.execute(full_query, params)
    rows = cur.fetchall()

    # Formatar a lista de pacientes
    patients = []
    for row in rows:
        row = list(row)
        birthdate = row[2]
        if isinstance(birthdate, datetime):
            row[2] = birthdate.strftime("%d/%m/%Y")
        elif isinstance(birthdate, str) and birthdate:
            try:
                row[2] = datetime.strptime(birthdate, "%Y-%m-%d").strftime("%d/%m/%Y")
            except ValueError:
                pass
        patients.append(row)

    cur.close()
    conn.close()

    return render_template(
        "estudos.html",
        SERVER_IP=SERVER_IP,
        patients=patients,
        visible_count=len(patients),
        total_records=total_records,
        page=page,
        per_page=per_page_param,  # Usar o valor original para o dropdown
        total_pages=total_pages,
        per_page_options=per_page_options,
        id_paciente=id_paciente,
        nome=nome,
        data_nascimento=data_nascimento,
        sexo=sexo,
        data_atendimento=data_atendimento,
        modalidade=modalidade,
        procedimento=procedimento,
        protocolo=protocolo,
        status=status,
        qtd_operador=qtd_operador,
        qtd_valor=qtd_valor,
        sort_by=sort_by,
        sort_order=sort_order,
        user_name=user_name,
        user_role=user_role,
        alert=alert
    )
