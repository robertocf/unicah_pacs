def get_user_permissions(user):
    """
    Retorna um dicionário de permissões para o usuário.
    No futuro, isso será carregado do banco de dados.
    """
    # Admin tem todas as permissões
    if getattr(user, 'role', None) == 'admin':
        return {
            'visualizar_estudos': True,
            'editar_estudos': True,
            'acessar_menu_configuracoes': True,
            'excluir_estudos': True,
            'imprimir_estudos': True,
            # Novos escopos de acesso
            'visualizar_relatorios': True,
            'acessar_gerencial': True,
            'criar_usuarios': True,
            'criar_empresas': True,
            'associar': True,
            'acessar_armazenamento': True,
            'acessar_permissoes': True,
        }

    # Padrão para outros usuários (pode ser ajustado posteriormente)
    return {
        'visualizar_estudos': True,
        'editar_estudos': False,
        'acessar_menu_configuracoes': False,
        'excluir_estudos': False,
        'imprimir_estudos': True,
        # Novos escopos de acesso - padrão restritivo
        'visualizar_relatorios': False,
        'acessar_gerencial': False,
        'criar_usuarios': False,
        'criar_empresas': False,
        'associar': False,
        'acessar_armazenamento': False,
        'acessar_permissoes': False,
    }


def list_permission_definitions():
    """Lista todas as permissões disponíveis com rótulos para exibição."""
    return [
        {'key': 'visualizar_estudos', 'label': 'Visualizar estudos'},
        {'key': 'editar_estudos', 'label': 'Editar estudos'},
        {'key': 'acessar_menu_configuracoes', 'label': 'Acessar menu configurações'},
        {'key': 'excluir_estudos', 'label': 'Excluir estudos'},
        {'key': 'imprimir_estudos', 'label': 'Imprimir estudos'},
        {'key': 'visualizar_relatorios', 'label': 'Relatórios'},
        {'key': 'acessar_gerencial', 'label': 'Gerencial'},
        {'key': 'criar_usuarios', 'label': 'Criar usuários'},
        {'key': 'criar_empresas', 'label': 'Criar empresas'},
        {'key': 'associar', 'label': 'Associar'},
        {'key': 'acessar_armazenamento', 'label': 'Armazenamento'},
        {'key': 'acessar_permissoes', 'label': 'Acessar permissões'},
    ]