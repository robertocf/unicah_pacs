import json
from datetime import datetime, date
from typing import Optional
import unicodedata
from db import get_db_connection

# Toggle de debug: quando True imprime comparações antes do INSERT
DEBUG = True


def _get_existing_patient_data(pat_id: str):
    """Busca os dados atuais do paciente no banco e já faz uma limpeza básica."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 
                pat_name,
                pat_birthdate,
                pat_sex
            FROM patient
            WHERE pat_id = %s
            """,
            (pat_id,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        # Normaliza tipos básicos (deixa como strings ou "")
        pat_name = str(row[0]) if row[0] is not None else ""
        # pat_birthdate pode ser date/datetime ou string
        pat_birthdate = None
        if isinstance(row[1], (datetime, date)):
            pat_birthdate = row[1].strftime("%Y-%m-%d")
        elif row[1] is not None:
            pat_birthdate = str(row[1])
        else:
            pat_birthdate = ""
        pat_sex = str(row[2]) if row[2] is not None else ""
        return {
            "pat_name": pat_name,
            "pat_birthdate": pat_birthdate,
            "pat_sex": pat_sex,
        }
    except Exception as e:
        print(f"Erro em _get_existing_patient_data: {e}")
        return {}
    finally:
        cur.close()
        conn.close()


def _get_patient_context_data(
    pat_id: str,
    nome_paciente: Optional[str] = None,
    paciente_birthdate: Optional[str] = None
):
    """Busca empresa, modalidade e data do último estudo."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
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
        row = cur.fetchone()
        if row:
            return {
                "empresa_id": row[0] or "",
                "modalidade_estudo": row[1] or "",
                "data_estudo": row[2],
            }

        # fallback simplificado
        return {"empresa_id": "", "modalidade_estudo": "", "data_estudo": None}

    except Exception as e:
        print(f"Erro em _get_patient_context_data: {e}")
        return {"empresa_id": "", "modalidade_estudo": "", "data_estudo": None}
    finally:
        cur.close()
        conn.close()


# -------- helpers de normalização / comparação --------
def _normalize_name(s: Optional[str]) -> str:
    """Normaliza nome: none->'', unicode normalize, casefold, colapsa espaços."""
    if s is None:
        return ""
    s = str(s)
    # Normaliza unicode (acentos)
    s = unicodedata.normalize("NFKC", s)
    # remove múltiplos espaços e trim
    s = " ".join(s.split())
    # case-insensitive comparison
    return s.casefold()


def _normalize_sex(s: Optional[str]) -> str:
    """Normaliza sexo: pega a primeira letra alfabética e uppercase (M/F/O), else ''. """
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    # Só considerar a primeira letra relevante
    for ch in s:
        if ch.isalpha():
            return ch.upper()
    return ""


def _normalize_date_to_yyyy_mm_dd(s: Optional[str]) -> str:
    """Tenta converter várias representações de data para 'YYYY-MM-DD'. Retorna '' se vazio/invalid."""
    if s is None:
        return ""
    if isinstance(s, (date, datetime)):
        return s.strftime("%Y-%m-%d")
    s = str(s).strip()
    if not s:
        return ""
    # Tenta parse com alguns formatos comuns
    formatos = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d", "%Y/%m/%d", "%d.%m.%Y"]
    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    # Se não conseguiu, retorna a string original (mas trimada) para comparação
    return s


def insert_log_registro(
    tipo_acao: str,
    paciente_id: str,
    nome_paciente: str,
    usuario_nome: str,
    paciente_birthdate: Optional[str] = None,
    paciente_sex: Optional[str] = None,
    empresa_id: Optional[str] = None,
    modalidade_estudo: Optional[str] = None,
    data_estudo: Optional[datetime] = None,
    contexto_extra: Optional[dict] = None,
    dados_atuais_anterior: Optional[dict] = None,
) -> bool:
    """Insere log de alterações com antes/depois apenas dos campos realmente alterados."""

    # 1) buscar dados atuais do paciente (ou usar snapshot anterior, se fornecido)
    dados_atuais = (dados_atuais_anterior or _get_existing_patient_data(paciente_id) or {})

    paciente = f"{nome_paciente}^^^^"
    # 2) normalizar antigos
    antigo_name_norm = _normalize_name(dados_atuais.get("pat_name", ""))
    antigo_sex_norm = _normalize_sex(dados_atuais.get("pat_sex", ""))
    antigo_birth_norm = _normalize_date_to_yyyy_mm_dd(dados_atuais.get("pat_birthdate", ""))

    # 3) normalizar novos (vindos do formulário)
    novo_name_norm = _normalize_name(paciente)
    novo_sex_norm = _normalize_sex(paciente_sex)
    novo_birth_norm = _normalize_date_to_yyyy_mm_dd(paciente_birthdate)
    if DEBUG:
        print("=== DEBUG comparacao campos ===")
        print("antigo raw:", dados_atuais)
        print("antigo normalized:", {
            "pat_name": antigo_name_norm,
            "pat_sex": antigo_sex_norm,
            "pat_birthdate": antigo_birth_norm,
        })
        print("novo raw:", {
            "pat_name": paciente,
            "pat_sex": paciente_sex,
            "pat_birthdate": paciente_birthdate,
        })
        print("novo normalized:", {
            "pat_name": novo_name_norm,
            "pat_sex": novo_sex_norm,
            "pat_birthdate": novo_birth_norm,
        })

    # 4) comparar campo a campo com regras específicas
    campos_alterados = {}

    # name: comparação casefold + collapse spaces (já feito)
    if novo_name_norm != antigo_name_norm:
        campos_alterados["pat_name"] = {
            "antes": dados_atuais.get("pat_name", "") or "",
            "depois": paciente or ""
        }

    # sex: comparação por letra normalizada (M/F/O)
    if novo_sex_norm != antigo_sex_norm:
        campos_alterados["pat_sex"] = {
            "antes": dados_atuais.get("pat_sex", "") or "",
            "depois": (paciente_sex or "")
        }

    # birthdate: comparar por YYYY-MM-DD (se possível)
    if novo_birth_norm != antigo_birth_norm:
        campos_alterados["pat_birthdate"] = {
            "antes": dados_atuais.get("pat_birthdate", "") or "",
            "depois": (paciente_birthdate or "")
        }

    # 5) Buscar dados complementares se faltarem (mantém sua lógica anterior)
    if not empresa_id or not modalidade_estudo or not data_estudo:
        ctx = _get_patient_context_data(
            paciente_id,
            nome_paciente=nome_paciente,
            paciente_birthdate=paciente_birthdate
        )
        empresa_id = empresa_id or ctx.get("empresa_id") or ""
        modalidade_estudo = modalidade_estudo or ctx.get("modalidade_estudo") or ""
        data_estudo = data_estudo or ctx.get("data_estudo")

    # Normaliza empresa_id para evitar string vazia em INSERT (ex.: DELETE)
    try:
        if empresa_id is None or str(empresa_id).strip() == "":
            empresa_id = 0
        else:
            empresa_id = int(empresa_id)
    except Exception:
        empresa_id = 0

    # 5.1) Se não houve nenhum campo alterado em UPDATE, não inserir log
    if tipo_acao.strip().upper() == "UPDATE" and not campos_alterados:
        if DEBUG:
            print("Nenhuma alteração detectada; não inserindo log de UPDATE.")
        return True

    # 6) montar contexto JSON com somente os campos alterados
    contexto = {
        "usuario_nome": usuario_nome,
        "campos_alterados": campos_alterados
    }
    if contexto_extra:
        contexto.update(contexto_extra)

    contexto_json = json.dumps(contexto, ensure_ascii=False)

    # Para DELETE, não precisamos de contexto preenchido
    if tipo_acao.strip().upper() == "DELETE":
        contexto_json = None

    if DEBUG:
        print("contexto final para gravar:", contexto_json)

    # 6.1) definir valor de nome_paciente
    # - UPDATE: somente se o nome foi alterado
    # - DELETE: sempre registrar o nome informado
    if tipo_acao.strip().upper() == "DELETE":
        nome_paciente_val = nome_paciente or None
    else:
        nome_paciente_val = nome_paciente if ("pat_name" in campos_alterados) else None

    # 7) inserir no banco
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO log_registros (
                data_hora,
                empresa_id,
                usuario_id,
                tipo_acao,
                paciente_id,
                nome_paciente,
                modalidade_estudo,
                data_estudo,
                contexto
            )
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                empresa_id,
                usuario_nome,
                tipo_acao,
                paciente_id,
                nome_paciente_val,
                modalidade_estudo,
                data_estudo,
                contexto_json,
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"Erro ao inserir log_registros: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def insert_login_log(usuario_nome: str, contexto_extra: Optional[dict] = None) -> bool:
    """Insere um log de login. Usa tipo_acao permitido pelo CHECK e marca evento='LOGIN' no contexto."""
    # Sempre marcar que é um login no contexto
    base_context = {"evento": "LOGIN"}
    if contexto_extra and isinstance(contexto_extra, dict):
        base_context.update(contexto_extra)
    try:
        contexto_json = json.dumps(base_context, ensure_ascii=False)
    except Exception:
        contexto_json = None

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO log_registros (
                data_hora,
                empresa_id,
                usuario_id,
                tipo_acao,
                paciente_id,
                nome_paciente,
                modalidade_estudo,
                data_estudo,
                contexto
            )
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                0,                  # empresa_id nulo
                usuario_nome,          # usuario_id
                'LOGIN',              # usar valor permitido pelo CHECK; evento real no contexto
                0,                  # paciente_id nulo
                None,                  # nome_paciente nulo
                None,                  # modalidade_estudo nulo
                None,                  # data_estudo nulo
                contexto_json,
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"Erro ao inserir log de LOGIN: {e}")
        return False
    finally:
        cur.close()
        conn.close()
