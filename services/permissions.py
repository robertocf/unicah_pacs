from models.RolePermission import RolePermission

def get_user_permissions(user):
    """
    Retorna um dicionário de permissões para o usuário, carregado do banco de dados.
    Inclui fallback para permissões hardcoded caso o banco esteja vazio.
    """
    role = (getattr(user, 'role', '') or '').lower()
    user_id = (getattr(user, 'user_id', '') or '').lower()
    name = (getattr(user, 'name', '') or '').lower()
    
    # Root sempre tem permissão total
    is_root = role == 'root' or user_id == 'root' or name == 'root'
    
    # Lista de todas as permissões possíveis
    all_defs = list_permission_definitions()
    all_keys = [d['key'] for d in all_defs]

    if is_root:
        return {key: True for key in all_keys}

    # Tenta carregar do banco de dados
    try:
        db_perms = RolePermission.query.filter_by(role_name=role).all()
        if db_perms:
            activated_keys = {p.permission_key for p in db_perms}
            return {key: (key in activated_keys) for key in all_keys}
    except Exception:
        # Se houver erro no banco (ex: tabela não criada), segue para o fallback
        pass

    # Fallback Hardcoded (Compatibilidade com grupos fixos atuais)
    if role == 'admin':
        return {
            'visualizar_estudos': True,
            'editar_estudos': True,
            'acessar_menu_configuracoes': True,
            'excluir_estudos': True,
            'imprimir_estudos': True,
            'acessar_importar_dicom': False,
            'visualizar_relatorios': True,
            'acessar_gerencial': True,
            'criar_usuarios': True,
            'criar_empresas': True,
            'associar': True,
            'acessar_armazenamento': True,
            'acessar_permissoes': True,
            'abrir_weasis_local': True,
            'abrir_weasis_servidor': True,
            'abrir_no_navegador': True,
            'baixar_imagens': True,
            'laudo': True,
            'imprimir_laudo': True,
            'acessar_modelos_laudo': True,
            'acessar_layout_laudo': True,
            'acessar_auto_texto': True,
            'ver_todos_estudos': True,
            'resetar_laudo': True,
            'acessar_lixeira': True,
            'excluir_permanente': True,
            'configurar_sistema': True,
        }
    
    # Padrão para outros (Médico, Técnico, etc) enquanto não configurado via UI
    return {
        'visualizar_estudos': True,
        'editar_estudos': False,
        'acessar_menu_configuracoes': False,
        'excluir_estudos': False,
        'imprimir_estudos': True,
        'acessar_importar_dicom': False,
        'visualizar_relatorios': False,
        'acessar_gerencial': False,
        'criar_usuarios': False,
        'criar_empresas': False,
        'associar': False,
        'acessar_armazenamento': False,
        'acessar_permissoes': False,
        'abrir_weasis_local': True,
        'abrir_weasis_servidor': True,
        'abrir_no_navegador': True,
        'baixar_imagens': True,
        'laudo': False,
        'imprimir_laudo': True,
        'acessar_menu_configuracoes': True,
        'acessar_auto_texto': True,
        'ver_todos_estudos': False,
    }


def list_permission_definitions():
    """Lista todas as permissões disponíveis com rótulos para exibição."""
    return [
        {'key': 'visualizar_estudos', 'label': 'Visualizar estudos'},
        {'key': 'editar_estudos', 'label': 'Editar estudos'},
        {'key': 'acessar_menu_configuracoes', 'label': 'Acessar menu configurações'},
        {'key': 'excluir_estudos', 'label': 'Excluir estudos'},
        {'key': 'imprimir_estudos', 'label': 'Imprimir imagens'},
        {'key': 'abrir_weasis_local', 'label': 'Abrir no Weasis (local)'},
        {'key': 'abrir_weasis_servidor', 'label': 'Abrir no Weasis (servidor)'},
        {'key': 'abrir_no_navegador', 'label': 'Abrir no navegador'},
        {'key': 'baixar_imagens', 'label': 'Baixar imagens'},
        {'key': 'visualizar_relatorios', 'label': 'Relatórios'},
        {'key': 'acessar_gerencial', 'label': 'Gerencial'},
        {'key': 'criar_usuarios', 'label': 'Criar usuários'},
        {'key': 'criar_empresas', 'label': 'Criar empresas'},
        {'key': 'associar', 'label': 'Associar'},
        {'key': 'acessar_armazenamento', 'label': 'Armazenamento'},
        {'key': 'acessar_permissoes', 'label': 'Acessar permissões'},
        {'key': 'laudo', 'label': 'Laudo'},
        {'key': 'imprimir_laudo', 'label': 'Imprimir laudo'},
        {'key': 'acessar_modelos_laudo', 'label': 'Modelos de laudo'},
        {'key': 'acessar_layout_laudo', 'label': 'Layout do laudo'},
        {'key': 'acessar_auto_texto', 'label': 'Auto texto'},
        {'key': 'ver_todos_estudos', 'label': 'Ver todos os estudos'},
        {'key': 'resetar_laudo', 'label': 'Resetar laudo'},
        {'key': 'acessar_lixeira', 'label': 'Acessar Lixeira'},
        {'key': 'excluir_permanente', 'label': 'Excluir Permanente (HDS/PACS)'},
        {'key': 'configurar_sistema', 'label': 'Configurações do Sistema'},
    ]
