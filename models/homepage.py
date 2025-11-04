from flask import request, render_template
from datetime import datetime
from config import SERVER_IP  # ajuste se o import do config for diferente
from db import get_db_connection  # ajuste se o import da conexão for diferente

def carregar_homepage(user_name, user_id, user_role=None, alert=None):  
    id_paciente = (
        request.form.get("id_paciente", "") if request.method == "POST" else ""
    )
    nome = request.form.get("nome", "") if request.method == "POST" else ""
    data_nascimento = (
        request.form.get("data_nascimento", "") if request.method == "POST" else ""
    )
    sexo = request.form.get("sexo", "") if request.method == "POST" else ""
    data_atendimento = (
        request.form.get("data_atendimento", "all")
        if request.method == "POST"
        else "all"
    )

    modalidade = (
        request.form.get("modalidade", "all") if request.method == "POST" else "all"
    )

    page = int(request.args.get("page", 1))
    per_page_param = (
        request.form.get("per_page", 10)
        if request.method == "POST"
        else request.args.get("per_page", 10)
    )
    
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
                   CASE s.study_custom1 WHEN 'I' THEN 'Impresso' WHEN 'V' THEN 'Visual' ELSE 'Pronto' END AS custom,
                   s.num_instances,
                   s.pk,
                   CASE WHEN sr.institution IS NULL THEN '' else sr.institution END AS institution, 
                   CASE WHEN  sr.station_name IS NULL THEN '' else  sr.station_name END AS  station_name,
                   CASE WHEN s.ref_physician IS NULL THEN '' else s.ref_physician END AS ref_physician,
                   CASE WHEN s.study_id IS NULL THEN '' else s.study_id END AS study_id,
                   s.study_iuid,
                   CASE WHEN s.accession_no IS NULL THEN '' else s.accession_no END AS accession_no
            FROM patient p
            JOIN study s ON s.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = s.pk
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
                   CASE s.study_custom1 WHEN 'I' THEN 'Impresso' WHEN 'V' THEN 'Visual' ELSE 'Pronto' END AS custom,
                   s.num_instances,
                   s.pk,
                   CASE WHEN sr.institution IS NULL THEN '' else sr.institution END AS institution, 
                   CASE WHEN  sr.station_name IS NULL THEN '' else  sr.station_name END AS  station_name,
                   CASE WHEN s.ref_physician IS NULL THEN '' else s.ref_physician END AS ref_physician,
                   CASE WHEN s.study_id IS NULL THEN '' else s.study_id END AS study_id,
                   s.study_iuid,
                   CASE WHEN s.accession_no IS NULL THEN '' else s.accession_no END AS accession_no
            FROM patient p
            JOIN study s ON s.patient_fk = p.pk
            JOIN series sr ON sr.study_fk = s.pk
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
            s.accession_no
    """

    # Condições dinâmicas
    params = [user_id] if user_role != 'admin' else []
    conditions = []

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
    if data_atendimento != "all":
        if data_atendimento == "today":
            conditions.append("s.study_datetime::date = CURRENT_DATE")
        elif data_atendimento == "last3days":
            conditions.append("s.study_datetime::date >= CURRENT_DATE - INTERVAL '3 days'")
        elif data_atendimento == "last30days":
            conditions.append("s.study_datetime::date >= CURRENT_DATE - INTERVAL '30 days'")

    if modalidade != "all":
        conditions.append("sr.modality = %s")
        params.append(modalidade)

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
        full_query += """
            ORDER BY s.study_datetime DESC
        """
    else:
        total_pages = (total_records + per_page - 1) // per_page
        full_query += """
            ORDER BY s.study_datetime DESC
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
        user_name=user_name,
        user_role=user_role,
        alert=alert
    )
