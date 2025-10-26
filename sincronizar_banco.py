import os
import json
import psycopg2
import re
import logging
from datetime import datetime
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env COM ENCODING EXPLÍCITO
# Tenta múltiplos encodings para garantir compatibilidade
try:
    load_dotenv(encoding='utf-8')
except:
    try:
        load_dotenv(encoding='latin-1')
    except:
        load_dotenv()  # Usa o padrão do sistema

# --- CONFIGURAÇÃO DO LOG ---
log_filename = 'sincronizacao.log'
if os.path.exists(log_filename):
    os.remove(log_filename)
    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- CONFIGURAÇÕES ---
PASTA_DOWNLOAD = "downloads"
PASTA_PROCESSADOS = os.path.join(PASTA_DOWNLOAD, "processados")
DB_URL = os.getenv('DB_URL')
TENANT_ID = 20
INSURANCE_COMPANY_ID = 44

# --- FUNÇÕES AUXILIARES ---
def clean_cpf(cpf):
    """Remove caracteres não numéricos de uma string de CPF."""
    if isinstance(cpf, str):
        return re.sub(r'[^0-9]', '', cpf)
    return cpf

def safe_str(value):
    """Garante que o valor seja uma string UTF-8 válida."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except:
        return None

def format_db_date(date_str):
    """Tenta formatar uma string de data para o formato YYYY-MM-DD."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
            try:
                return datetime.strptime(str(date_str), fmt).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass
        return None
    except Exception:
        return None

def calculate_delay_days(vencimento_original, vencimento_atual=None):
    """Calcula o número de dias em atraso com base na data de vencimento."""
    try:
        data_vencimento_str = vencimento_atual if vencimento_atual else vencimento_original
        if not data_vencimento_str:
            return 0
        
        data_formatada = format_db_date(data_vencimento_str)
        if not data_formatada:
            return 0

        data_venc = datetime.strptime(data_formatada, '%Y-%m-%d')
        data_hoje = datetime.now()
        if data_hoje > data_venc:
            delay = (data_hoje - data_venc).days
            return delay
        return 0
    except (ValueError, TypeError):
        return 0
        
def extrair_nome_corretora_do_arquivo(filename):
    """Extrai o nome da corretora a partir do nome do arquivo JSON."""
    try:
        nome_parcial = filename.replace("Extracao_", "").replace("_backup.json", "")
        parts = nome_parcial.split('_')
        nome_parts = parts[:-2]
        nome_em_maiusculo = ' '.join(nome_parts)
        return nome_em_maiusculo.title()
    except Exception:
        return "NOME_DESCONHECIDO"

def identificar_tipo_dados(elemento):
    """Identifica o tipo de dados contido em uma seção do JSON (clientes, propostas, etc.)."""
    if isinstance(elemento, dict) and 'name' in elemento:
        nome_secao = elemento['name'].lower()
        if 'clientes' in nome_secao:
            return 'clientes'
        elif 'propostas' in nome_secao or 'status' in nome_secao:
            return 'propostas'
        elif 'pendentes' in nome_secao or 'inadimplentes' in nome_secao:
            return 'inadimplentes'
        elif 'produtos' in nome_secao and 'vida' in nome_secao:
            return 'produtos_vida'
        elif 'produtos' in nome_secao and 'previdencia' in nome_secao:
            return 'produtos_previdencia'
    return 'desconhecido'

def extrair_dados(elemento):
    """Extrai a lista de dados da chave 'data' de um elemento do JSON."""
    if isinstance(elemento, dict) and 'data' in elemento:
        return elemento['data']
    return []

# --- FUNÇÕES DE BANCO DE DADOS ---
def get_db_connection():
    """Estabelece e retorna uma conexão com o banco de dados."""
    try:
        if not DB_URL:
            logging.error("A variável de ambiente DB_URL não foi definida.")
            return None
        
        # LOG para debug - mostra primeiros caracteres da URL (sem senha)
        url_parts = DB_URL.split('@')
        if len(url_parts) > 1:
            logging.info(f"Tentando conectar ao servidor: {url_parts[-1][:30]}...")
        
        # Conecta diretamente sem conversão de encoding
        conn = psycopg2.connect(DB_URL)
        conn.set_client_encoding('UTF8')
        logging.info("Conexão com banco estabelecida com sucesso.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao conectar: {e}")
        return None

def get_client_id(cursor, cpf, tenant_id):
    """Busca e retorna o ID de um cliente pelo CPF e tenant_id."""
    try:
        cursor.execute('SELECT id FROM public.clients WHERE documento = %s AND tenant_id = %s', (cpf, tenant_id))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Erro ao buscar client ID para CPF {cpf}: {e}")
        return None

def get_or_create_client_id(cur, cpf, nome, broker_id, tenant_id):
    """Busca um cliente pelo CPF. Se não encontrar, cria um novo cliente simplificado."""
    client_id = get_client_id(cur, cpf, tenant_id)
    if client_id:
        return client_id
    try:
        logging.info(f"CPF {cpf} não encontrado. Criando novo cliente simplificado: {nome}")
        sql = "INSERT INTO public.clients (tenant_id, nome, tipo_documento, documento, broker_id) VALUES (%s, %s, %s, %s, %s) RETURNING id;"
        params = (tenant_id, safe_str(nome), 'CPF', cpf, broker_id)
        cur.execute(sql, params)
        new_client_id = cur.fetchone()[0]
        logging.info(f"Novo cliente criado com ID: {new_client_id}")
        return new_client_id
    except Exception as e:
        logging.error(f"Erro ao criar cliente simplificado para CPF {cpf}: {e}")
        cur.connection.rollback()
        return None

def salvar_clientes_no_banco(clientes, corretora_nome, db_url):
    """Salva uma lista de novos clientes no banco de dados."""
    logging.info(f"Iniciando sincronização de clientes para '{corretora_nome}'...")
    conn = get_db_connection()
    if not conn: return None
    broker_id = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM public.brokers WHERE UPPER(nome_completo) LIKE UPPER(%s)", (f"{corretora_nome}%",))
        broker_row = cur.fetchone()
        if not broker_row:
            logging.error(f"Erro: Broker com nome similar a '{corretora_nome}' não encontrado.")
            return None
        broker_id = broker_row[0]
        logging.info(f"Broker ID para '{corretora_nome}' é {broker_id}.")
        clientes_inseridos = 0
        for i, cliente in enumerate(clientes):
            try:
                if not isinstance(cliente, dict): continue
                doc_numero = clean_cpf(cliente.get('documento', ''))
                if not doc_numero or len(doc_numero) < 11: continue
                if get_client_id(cur, doc_numero, TENANT_ID): continue
                
                sql_insert = """
                    INSERT INTO public.clients (
                        tenant_id, nome, tipo_documento, documento, broker_id, data_nascimento, 
                        telefone, email, endereco, cidade, cep, titular_cpf, sexo, estado_civil, 
                        numero_documento, orgao_expedidor, renda_patrimonio, profissao, numero, 
                        complemento, bairro, uf
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = (
                    TENANT_ID, safe_str(cliente.get('nome')), 'CPF', doc_numero, broker_id, 
                    format_db_date(cliente.get('data_nascimento')), safe_str(cliente.get('telefone')), 
                    safe_str(cliente.get('email')), safe_str(cliente.get('endereco')), 
                    safe_str(cliente.get('cidade')), safe_str(cliente.get('cep')), 
                    cliente.get('titular_cpf'), safe_str(cliente.get('sexo')), 
                    safe_str(cliente.get('estado_civil')), safe_str(cliente.get('numero_documento')), 
                    safe_str(cliente.get('orgao_expedidor')), safe_str(cliente.get('renda_patrimonio')), 
                    safe_str(cliente.get('profissao')), safe_str(cliente.get('numero')), 
                    safe_str(cliente.get('complemento')), safe_str(cliente.get('bairro')), 
                    safe_str(cliente.get('uf'))
                )
                cur.execute(sql_insert, params)
                clientes_inseridos += 1
            except Exception as e:
                logging.error(f"Erro ao processar cliente na linha {i+1}: {e}")
                conn.rollback()
        conn.commit()
        logging.info(f"Sincronização de clientes concluída. {clientes_inseridos} novos clientes inseridos.")
        return broker_id
    except Exception as e: 
        logging.error(f"Erro na operação com banco de dados (clientes): {e}")
        if conn: conn.rollback()
        return None
    finally:
        if conn: conn.close()

def salvar_propostas_no_banco(propostas, broker_id, id_cpf_map, db_url):
    """Salva ou atualiza propostas no banco de dados."""
    logging.info("Iniciando sincronização de Status Propostas...")
    if not propostas:
        logging.info("Nenhuma proposta para sincronizar.")
        return
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        propostas_inseridas = 0
        propostas_atualizadas = 0
        propostas_puladas = 0
        propostas_existentes = 0

        for i, prop in enumerate(propostas):
            try:
                if not isinstance(prop, dict): continue
                num_proposta = prop.get('proposta', '')
                if not num_proposta: 
                    propostas_puladas += 1
                    continue
                
                cpf_cliente = None
                source_client_id = prop.get('id_cliente') 
                if source_client_id and id_cpf_map:
                     cpf_cliente = id_cpf_map.get(source_client_id)
                
                if not cpf_cliente:
                    cpf_cliente = clean_cpf(prop.get('cpf'))

                if not cpf_cliente:
                    logging.warning(f"(Propostas) Linha {i+1}: Não foi possível encontrar CPF para a proposta '{num_proposta}'. Pulando.")
                    propostas_puladas +=1
                    continue
                
                data_vencimento = format_db_date(prop.get('vencimento'))
                if not data_vencimento:
                    logging.warning(f"Proposta {num_proposta} pulada por não ter data de vencimento válida.")
                    propostas_puladas +=1
                    continue

                nome_cliente = safe_str(prop.get('nome', 'Cliente não informado'))
                client_id = get_or_create_client_id(cur, cpf_cliente, nome_cliente, broker_id, TENANT_ID)
                if not client_id:
                    logging.error(f"(Propostas) Linha {i+1}: Não foi possível obter/criar um client_id para a proposta {num_proposta}. Pulando.")
                    continue
                
                sql_check = """
                    SELECT id, status_proposta, forma_pagamento, valor, vencimento, 
                           competencia, status_pagamento, motivo_pendencia
                    FROM public.proposals 
                    WHERE proposta = %s AND tenant_id = %s
                """
                cur.execute(sql_check, (str(num_proposta), TENANT_ID))
                existing_record = cur.fetchone()
                
                if existing_record:
                    existing_id = existing_record[0]
                    existing_status_proposta = existing_record[1]
                    existing_forma_pagamento = existing_record[2]
                    existing_valor = existing_record[3]
                    existing_vencimento = existing_record[4]
                    existing_competencia = existing_record[5]
                    existing_status_pagamento = existing_record[6]
                    existing_motivo_pendencia = existing_record[7]
                    
                    has_changes = (
                        existing_status_proposta != safe_str(prop.get('status_proposta')) or
                        existing_forma_pagamento != safe_str(prop.get('forma_pagamento')) or
                        existing_valor != prop.get('valor', 0.0) or
                        existing_vencimento != data_vencimento or
                        existing_competencia != safe_str(prop.get('competencia')) or
                        existing_status_pagamento != safe_str(prop.get('status_pagamento')) or
                        existing_motivo_pendencia != safe_str(prop.get('motivo_pendencia'))
                    )
                    
                    if has_changes:
                        sql_update = """
                            UPDATE public.proposals SET
                                client_id = %s,
                                broker_id = %s,
                                insurance_company_id = %s,
                                produto = %s,
                                linha_negocio = %s,
                                criada_em = %s,
                                status_proposta = %s,
                                forma_pagamento = %s,
                                valor = %s,
                                vencimento = %s,
                                competencia = %s,
                                status_pagamento = %s,
                                motivo_pendencia = %s,
                                "data" = %s
                            WHERE id = %s
                        """
                        cur.execute(sql_update, (
                            client_id,
                            broker_id,
                            INSURANCE_COMPANY_ID,
                            safe_str(prop.get('produto')),
                            safe_str(prop.get('linha_negocio')),
                            format_db_date(prop.get('criada_em')),
                            safe_str(prop.get('status_proposta')),
                            safe_str(prop.get('forma_pagamento')),
                            prop.get('valor', 0.0),
                            data_vencimento,
                            safe_str(prop.get('competencia')),
                            safe_str(prop.get('status_pagamento')),
                            safe_str(prop.get('motivo_pendencia')),
                            format_db_date(prop.get('data')),
                            existing_id
                        ))
                        propostas_atualizadas += 1
                    else:
                        propostas_existentes += 1
                    continue
                
                sql = """
                    INSERT INTO public.proposals (
                        client_id, broker_id, tenant_id, insurance_company_id, produto, 
                        linha_negocio, proposta, criada_em, status_proposta, forma_pagamento, 
                        valor, vencimento, competencia, status_pagamento, motivo_pendencia, "data"
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                params = (
                    client_id, broker_id, TENANT_ID, INSURANCE_COMPANY_ID, 
                    safe_str(prop.get('produto')), safe_str(prop.get('linha_negocio')), 
                    str(num_proposta), format_db_date(prop.get('criada_em')), 
                    safe_str(prop.get('status_proposta')), safe_str(prop.get('forma_pagamento')), 
                    prop.get('valor', 0.0), data_vencimento, safe_str(prop.get('competencia')),
                    safe_str(prop.get('status_pagamento')), safe_str(prop.get('motivo_pendencia')),
                    format_db_date(prop.get('data'))
                )
                cur.execute(sql, params)
                propostas_inseridas += 1
            except Exception as e:
                logging.error(f"Erro ao processar proposta na linha {i+1}: {e}")
                conn.rollback()
        
        conn.commit()
        logging.info(f"Sincronização de Propostas concluída. Inseridas: {propostas_inseridas}, Atualizadas: {propostas_atualizadas}, Existentes (sem mudança): {propostas_existentes}, Puladas: {propostas_puladas}.")
    except Exception as e:
        logging.error(f"Erro na operação com banco de dados (propostas): {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def salvar_inadimplentes_no_banco(inadimplentes_data, corretora_nome, id_cpf_map, db_url):
    """Salva ou atualiza registros de inadimplência no banco de dados."""
    logging.info("Iniciando sincronização de Pagamentos Pendentes...")
    if not inadimplentes_data:
        logging.info("Nenhum pagamento pendente para sincronizar.")
        return
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        registros_inseridos = 0
        registros_atualizados = 0
        registros_pulados = 0
        registros_existentes = 0
        registros_sem_atraso = 0

        for i, registro in enumerate(inadimplentes_data):
            try:
                if not isinstance(registro, dict): 
                    registros_pulados += 1
                    continue
                
                cpf_cliente = clean_cpf(registro.get('cpf_cliente', ''))
                if not cpf_cliente:
                    source_client_id = registro.get('id_cliente')
                    if source_client_id:
                        cpf_cliente = id_cpf_map.get(source_client_id)

                if not cpf_cliente: 
                    logging.warning(f"(Inadimplentes) Linha {i+1}: Não foi possível encontrar CPF. Pulando.")
                    registros_pulados += 1
                    continue
                
                client_id = get_client_id(cur, cpf_cliente, TENANT_ID)
                if not client_id:
                    logging.warning(f"(Inadimplentes) Cliente com CPF {cpf_cliente} não encontrado no banco. Pulando.")
                    registros_pulados += 1
                    continue
                
                delay_days = calculate_delay_days(
                    registro.get('vencimento_original'),
                    registro.get('vencimento_atual')
                )

                if delay_days <= 0:
                    registros_sem_atraso += 1
                    continue

                numero_proposta = safe_str(registro.get('numero_proposta', ''))
                numero_certificado = safe_str(registro.get('numero_certificado', ''))
                competencia = safe_str(registro.get('competencia', ''))
                
                sql_check = """
                    SELECT id, original_due_date, current_due_date, contribution_value, 
                           payment_status, payment_method, delay_days
                    FROM public.defaulters_detailed 
                    WHERE client_id = %s AND tenant_id = %s AND proposal_number = %s 
                    AND certificate_number = %s AND competency = %s
                """
                cur.execute(sql_check, (client_id, TENANT_ID, numero_proposta, numero_certificado, competencia))
                existing_record = cur.fetchone()
                
                if existing_record:
                    existing_id = existing_record[0]
                    existing_original_due = existing_record[1]
                    existing_current_due = existing_record[2]
                    existing_contribution = existing_record[3]
                    existing_payment_status = existing_record[4]
                    existing_payment_method = existing_record[5]
                    existing_delay = existing_record[6]
                    
                    has_changes = (
                        existing_original_due != format_db_date(registro.get('vencimento_original')) or
                        existing_current_due != format_db_date(registro.get('vencimento_atual')) or
                        existing_contribution != registro.get('contribuicao', 0.0) or
                        existing_payment_status != safe_str(registro.get('status_pagamento', '')) or
                        existing_payment_method != safe_str(registro.get('forma_pagamento', '')) or
                        existing_delay != delay_days
                    )
                    
                    if has_changes:
                        sql_update = """
                            UPDATE public.defaulters_detailed SET
                                broker_name = %s,
                                client_name = %s,
                                client_cpf = %s,
                                business_line = %s,
                                product_name = %s,
                                original_due_date = %s,
                                current_due_date = %s,
                                contribution_value = %s,
                                payment_status = %s,
                                payment_method = %s,
                                delay_days = %s
                            WHERE id = %s
                        """
                        cur.execute(sql_update, (
                            safe_str(corretora_nome),
                            safe_str(registro.get('nome_cliente', '')),
                            cpf_cliente,
                            safe_str(registro.get('linha_negocio', '')),
                            safe_str(registro.get('produto', '')),
                            format_db_date(registro.get('vencimento_original')),
                            format_db_date(registro.get('vencimento_atual')),
                            registro.get('contribuicao', 0.0),
                            safe_str(registro.get('status_pagamento', '')),
                            safe_str(registro.get('forma_pagamento', '')),
                            delay_days,
                            existing_id
                        ))
                        registros_atualizados += 1
                    else:
                        registros_existentes += 1
                    continue

                sql = """
                    INSERT INTO public.defaulters_detailed (
                        tenant_id, client_id, broker_name, client_name, client_cpf, 
                        business_line, product_name, competency, original_due_date, 
                        current_due_date, contribution_value, proposal_number, 
                        certificate_number, payment_status, payment_method, delay_days
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                params = (
                    TENANT_ID, client_id, safe_str(corretora_nome),
                    safe_str(registro.get('nome_cliente', '')), cpf_cliente,
                    safe_str(registro.get('linha_negocio', '')), safe_str(registro.get('produto', '')),
                    competencia, format_db_date(registro.get('vencimento_original')),
                    format_db_date(registro.get('vencimento_atual')),
                    registro.get('contribuicao', 0.0), numero_proposta, 
                    numero_certificado, safe_str(registro.get('status_pagamento', '')), 
                    safe_str(registro.get('forma_pagamento', '')), delay_days
                )
                
                cur.execute(sql, params)
                registros_inseridos += 1
            except Exception as e:
                logging.error(f"Erro ao processar registro de inadimplência na linha {i+1}: {e}")
                conn.rollback()
        
        conn.commit()
        logging.info(f"Sincronização de Inadimplentes concluída. Inseridos: {registros_inseridos}, Atualizados: {registros_atualizados}, Existentes (sem mudança): {registros_existentes}, Sem atraso: {registros_sem_atraso}, Pulados: {registros_pulados}.")
        
    except Exception as e:
        logging.error(f"Erro na operação com banco de dados (inadimplentes): {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def salvar_produtos_vida_no_banco(produtos_data, broker_id, corretora_nome, id_cpf_map, db_url):
    """Salva/atualiza uma lista de produtos de Vida no banco de dados."""
    logging.info("Iniciando sincronização de Produtos Vida...")
    if not produtos_data:
        logging.info("Nenhum produto Vida para sincronizar.")
        return
        
    conn = get_db_connection()
    if not conn: return
    
    try:
        cur = conn.cursor()
        produtos_inseridos = 0
        produtos_atualizados = 0
        produtos_pulados_dados = 0
        produtos_cancelados = 0
        produtos_existentes = 0

        for i, produto in enumerate(produtos_data):
            try:
                if not isinstance(produto, dict):
                    produtos_pulados_dados += 1
                    continue

                situacao_produto = safe_str(produto.get('situacao_produto', '')).upper()
                if situacao_produto == 'CANCELADO':
                    produtos_cancelados += 1
                    continue

                source_client_id = produto.get('id_cliente')
                cpf_cliente = id_cpf_map.get(source_client_id)

                if not cpf_cliente:
                    logging.warning(f"(Vida) Linha {i+1}: Não foi possível encontrar o CPF para o id_cliente '{source_client_id}'. Pulando.")
                    produtos_pulados_dados += 1
                    continue

                nome_cliente = 'Cliente não informado'
                client_id = get_or_create_client_id(cur, cpf_cliente, nome_cliente, broker_id, TENANT_ID)
                if not client_id:
                    logging.error(f"(Vida) Linha {i+1}: Não foi possível obter/criar ID do cliente. Pulando.")
                    produtos_pulados_dados += 1
                    continue
                
                params_data = {
                    'tenant_id': TENANT_ID,
                    'client_id': client_id,
                    'broker_name': safe_str(corretora_nome),
                    'business_line': safe_str(produto.get('linha_negocio')),
                    'product_type': safe_str(produto.get('tipo_produto')),
                    'proposal_number': safe_str(produto.get('numero_proposta')),
                    'certificate_number': safe_str(produto.get('numero_certificado')),
                    'product_status': safe_str(produto.get('situacao_produto')),
                    'coverage_name': safe_str(produto.get('nome_cobertura')),
                    'insured_capital': produto.get('capital_segurado', 0.0),
                    'coverage_payment_period': safe_str(produto.get('periodo_pagamento_cobertura')),
                    'due_day': safe_str(produto.get('dia_vencimento')),
                    'last_payment': format_db_date(produto.get('ultimo_pagamento')),
                    'next_payment': format_db_date(produto.get('proximo_pagamento')),
                    'paid_installments_quantity': safe_str(produto.get('quantidade_parcelas_pagas')),
                    'pending_installments_quantity': safe_str(produto.get('quantidade_parcelas_pendentes')),
                    'payment_frequency': safe_str(produto.get('periodicidade_pagamentos'))
                }

                sql_check = """
                    SELECT id, product_status, insured_capital, last_payment, next_payment,
                           paid_installments_quantity, pending_installments_quantity
                    FROM public.products_clients 
                    WHERE tenant_id = %s 
                    AND client_id = %s 
                    AND proposal_number = %s 
                    AND certificate_number = %s
                    AND coverage_name = %s
                """
                cur.execute(sql_check, (
                    params_data['tenant_id'],
                    params_data['client_id'],
                    params_data['proposal_number'],
                    params_data['certificate_number'],
                    params_data['coverage_name']
                ))
                
                existing_record = cur.fetchone()
                
                if existing_record:
                    existing_id = existing_record[0]
                    existing_status = existing_record[1]
                    existing_capital = existing_record[2]
                    existing_last_payment = existing_record[3]
                    existing_next_payment = existing_record[4]
                    existing_paid = existing_record[5]
                    existing_pending = existing_record[6]
                    
                    has_changes = (
                        existing_status != params_data['product_status'] or
                        existing_capital != params_data['insured_capital'] or
                        existing_last_payment != params_data['last_payment'] or
                        existing_next_payment != params_data['next_payment'] or
                        existing_paid != params_data['paid_installments_quantity'] or
                        existing_pending != params_data['pending_installments_quantity']
                    )
                    
                    if has_changes:
                        sql_update = """
                            UPDATE public.products_clients SET
                                broker_name = %s,
                                business_line = %s,
                                product_type = %s,
                                product_status = %s,
                                coverage_name = %s,
                                insured_capital = %s,
                                coverage_payment_period = %s,
                                due_day = %s,
                                last_payment = %s,
                                next_payment = %s,
                                paid_installments_quantity = %s,
                                pending_installments_quantity = %s,
                                payment_frequency = %s
                            WHERE id = %s
                        """
                        cur.execute(sql_update, (
                            params_data['broker_name'],
                            params_data['business_line'],
                            params_data['product_type'],
                            params_data['product_status'],
                            params_data['coverage_name'],
                            params_data['insured_capital'],
                            params_data['coverage_payment_period'],
                            params_data['due_day'],
                            params_data['last_payment'],
                            params_data['next_payment'],
                            params_data['paid_installments_quantity'],
                            params_data['pending_installments_quantity'],
                            params_data['payment_frequency'],
                            existing_id
                        ))
                        produtos_atualizados += 1
                    else:
                        produtos_existentes += 1
                    continue
                
                sql_insert = """
                    INSERT INTO public.products_clients (
                        tenant_id, client_id, broker_name, business_line, product_type,
                        proposal_number, certificate_number, product_status, coverage_name,
                        insured_capital, coverage_payment_period, due_day, last_payment,
                        next_payment, paid_installments_quantity, pending_installments_quantity,
                        payment_frequency
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cur.execute(sql_insert, (
                    params_data['tenant_id'],
                    params_data['client_id'],
                    params_data['broker_name'],
                    params_data['business_line'],
                    params_data['product_type'],
                    params_data['proposal_number'],
                    params_data['certificate_number'],
                    params_data['product_status'],
                    params_data['coverage_name'],
                    params_data['insured_capital'],
                    params_data['coverage_payment_period'],
                    params_data['due_day'],
                    params_data['last_payment'],
                    params_data['next_payment'],
                    params_data['paid_installments_quantity'],
                    params_data['pending_installments_quantity'],
                    params_data['payment_frequency']
                ))
                produtos_inseridos += 1

            except Exception as e:
                logging.error(f"Erro ao processar registro de produto Vida na linha {i+1}: {e}")
                conn.rollback()

        conn.commit()
        logging.info(f"Sincronização de Produtos Vida concluída. Inseridos: {produtos_inseridos}, Atualizados: {produtos_atualizados}, Existentes (sem mudança): {produtos_existentes}, Cancelados (ignorados): {produtos_cancelados}, Pulados por dados: {produtos_pulados_dados}.")

    except Exception as e:
        logging.error(f"Erro fatal na operação com banco de dados (produtos vida): {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
        
def salvar_produtos_previdencia_no_banco(produtos_data, broker_id, corretora_nome, id_cpf_map, db_url):
    """Salva/atualiza uma lista de produtos de Previdência no banco de dados."""
    logging.info("Iniciando sincronização de Produtos Previdência...")
    if not produtos_data:
        logging.info("Nenhum produto Previdência para sincronizar.")
        return

    conn = get_db_connection()
    if not conn: return
    
    try:
        cur = conn.cursor()
        produtos_inseridos = 0
        produtos_atualizados = 0
        produtos_pulados_dados = 0
        produtos_cancelados = 0
        produtos_existentes = 0

        for i, produto in enumerate(produtos_data):
            try:
                if not isinstance(produto, dict):
                    produtos_pulados_dados += 1
                    continue

                situacao_produto = safe_str(produto.get('situacao_produto', 'Ativo')).upper()
                if situacao_produto == 'CANCELADO':
                    produtos_cancelados += 1
                    continue

                source_client_id = produto.get('id_cliente')
                cpf_cliente = id_cpf_map.get(source_client_id)

                if not cpf_cliente:
                    logging.warning(f"(Previdência) Linha {i+1}: Não foi possível encontrar o CPF para o id_cliente '{source_client_id}'. Pulando.")
                    produtos_pulados_dados += 1
                    continue
                
                nome_cliente = 'Cliente não informado'
                client_id = get_or_create_client_id(cur, cpf_cliente, nome_cliente, broker_id, TENANT_ID)
                if not client_id:
                    logging.error(f"(Previdência) Linha {i+1}: Não foi possível obter/criar ID do cliente. Pulando.")
                    produtos_pulados_dados += 1
                    continue
                
                reserva_bruta_valor = produto.get('reserva_bruta', 0.0)
                try:
                    capital = float(reserva_bruta_valor or 0.0)
                except (ValueError, TypeError):
                    capital = 0.0
                
                params_data = {
                    'tenant_id': TENANT_ID,
                    'client_id': client_id,
                    'broker_name': safe_str(corretora_nome),
                    'business_line': safe_str(produto.get('linha_negocio', 'Previdência')),
                    'product_type': safe_str(produto.get('tipo_produto')),
                    'proposal_number': safe_str(produto.get('numero_proposta')),
                    'certificate_number': safe_str(produto.get('numero_certificado', produto.get('numero_proposta'))),
                    'product_status': safe_str(produto.get('situacao_produto', 'Ativo')),
                    'coverage_name': safe_str(produto.get('nome_cobertura', 'Plano de Previdência')),
                    'insured_capital': capital,
                    'coverage_payment_period': safe_str(produto.get('periodo_pagamento_cobertura')),
                    'due_day': safe_str(produto.get('dia_vencimento')),
                    'last_payment': format_db_date(produto.get('ultima_contribuicao')),
                    'next_payment': None,
                    'paid_installments_quantity': safe_str(produto.get('quantidade_parcelas_pagas')),
                    'pending_installments_quantity': safe_str(produto.get('quantidade_parcelas_pendentes')),
                    'payment_frequency': safe_str(produto.get('periodicidade_pagamentos'))
                }

                sql_check = """
                    SELECT id, product_status, insured_capital, last_payment,
                           paid_installments_quantity, pending_installments_quantity
                    FROM public.products_clients 
                    WHERE tenant_id = %s 
                    AND client_id = %s 
                    AND proposal_number = %s 
                    AND certificate_number = %s
                    AND coverage_name = %s
                """
                cur.execute(sql_check, (
                    params_data['tenant_id'],
                    params_data['client_id'],
                    params_data['proposal_number'],
                    params_data['certificate_number'],
                    params_data['coverage_name']
                ))
                
                existing_record = cur.fetchone()
                
                if existing_record:
                    existing_id = existing_record[0]
                    existing_status = existing_record[1]
                    existing_capital = existing_record[2]
                    existing_last_payment = existing_record[3]
                    existing_paid = existing_record[4]
                    existing_pending = existing_record[5]
                    
                    has_changes = (
                        existing_status != params_data['product_status'] or
                        existing_capital != params_data['insured_capital'] or
                        existing_last_payment != params_data['last_payment'] or
                        existing_paid != params_data['paid_installments_quantity'] or
                        existing_pending != params_data['pending_installments_quantity']
                    )
                    
                    if has_changes:
                        sql_update = """
                            UPDATE public.products_clients SET
                                broker_name = %s,
                                business_line = %s,
                                product_type = %s,
                                product_status = %s,
                                coverage_name = %s,
                                insured_capital = %s,
                                coverage_payment_period = %s,
                                due_day = %s,
                                last_payment = %s,
                                next_payment = %s,
                                paid_installments_quantity = %s,
                                pending_installments_quantity = %s,
                                payment_frequency = %s
                            WHERE id = %s
                        """
                        cur.execute(sql_update, (
                            params_data['broker_name'],
                            params_data['business_line'],
                            params_data['product_type'],
                            params_data['product_status'],
                            params_data['coverage_name'],
                            params_data['insured_capital'],
                            params_data['coverage_payment_period'],
                            params_data['due_day'],
                            params_data['last_payment'],
                            params_data['next_payment'],
                            params_data['paid_installments_quantity'],
                            params_data['pending_installments_quantity'],
                            params_data['payment_frequency'],
                            existing_id
                        ))
                        produtos_atualizados += 1
                    else:
                        produtos_existentes += 1
                    continue

                sql_insert = """
                    INSERT INTO public.products_clients (
                        tenant_id, client_id, broker_name, business_line, product_type,
                        proposal_number, certificate_number, product_status, coverage_name,
                        insured_capital, coverage_payment_period, due_day, last_payment,
                        next_payment, paid_installments_quantity, pending_installments_quantity,
                        payment_frequency
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cur.execute(sql_insert, (
                    params_data['tenant_id'],
                    params_data['client_id'],
                    params_data['broker_name'],
                    params_data['business_line'],
                    params_data['product_type'],
                    params_data['proposal_number'],
                    params_data['certificate_number'],
                    params_data['product_status'],
                    params_data['coverage_name'],
                    params_data['insured_capital'],
                    params_data['coverage_payment_period'],
                    params_data['due_day'],
                    params_data['last_payment'],
                    params_data['next_payment'],
                    params_data['paid_installments_quantity'],
                    params_data['pending_installments_quantity'],
                    params_data['payment_frequency']
                ))
                produtos_inseridos += 1
            except Exception as e:
                numero_proposta_erro = produto.get('numero_proposta', 'N/A')
                logging.error(f"Erro ao processar registro de produto Previdência na linha {i+1} (Proposta: {numero_proposta_erro}): {e}")
                conn.rollback()

        conn.commit()
        logging.info(f"Sincronização de Produtos Previdência concluída. Inseridos: {produtos_inseridos}, Atualizados: {produtos_atualizados}, Existentes (sem mudança): {produtos_existentes}, Cancelados (ignorados): {produtos_cancelados}, Pulados por dados: {produtos_pulados_dados}.")

    except Exception as e:
        logging.error(f"Erro fatal na operação com banco de dados (produtos previdência): {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

# --- FUNÇÃO PRINCIPAL ---
def main():
    """Função principal que orquestra o processo de sincronização."""
    logging.info("--- INICIANDO SCRIPT DE SINCRONIZAÇÃO COM BANCO DE DADOS ---")
    if not os.path.exists(PASTA_DOWNLOAD):
        os.makedirs(PASTA_DOWNLOAD)
        logging.info(f"Pasta '{PASTA_DOWNLOAD}' não encontrada, criada.")
        return
    if not os.path.exists(PASTA_PROCESSADOS):
        os.makedirs(PASTA_PROCESSADOS)
    
    arquivos_para_processar = [f for f in os.listdir(PASTA_DOWNLOAD) if f.endswith("_backup.json")]
    if not arquivos_para_processar:
        logging.info("Nenhum novo arquivo .json para sincronizar.")
        return

    for filename in arquivos_para_processar:
        caminho_arquivo = os.path.join(PASTA_DOWNLOAD, filename)
        logging.info(f"\n{'='*80}\nProcessando arquivo: {filename}\n{'='*80}")
        
        all_sheets_data = None
        
        try:
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings_to_try:
                try:
                    with open(caminho_arquivo, 'r', encoding=encoding) as f:
                        all_sheets_data = json.load(f)
                    logging.info(f"Arquivo lido com sucesso usando encoding: {encoding}")
                    break
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    if encoding == encodings_to_try[-1]:
                        raise
                    continue
        except Exception as e:
            logging.error(f"Erro CRÍTICO ao ler ou decodificar o arquivo '{filename}': {e}")
            continue

        try:
            corretora_nome = extrair_nome_corretora_do_arquivo(filename)
            clientes_data, propostas_data, inadimplentes_data = None, None, None
            produtos_vida_data, produtos_previdencia_data = None, None
            
            if isinstance(all_sheets_data, list):
                for elemento in all_sheets_data:
                    tipo_dados = identificar_tipo_dados(elemento)
                    dados_extraidos = extrair_dados(elemento)
                    if tipo_dados == 'clientes':
                        clientes_data = dados_extraidos
                    elif tipo_dados == 'propostas':
                        propostas_data = dados_extraidos
                    elif tipo_dados == 'inadimplentes':
                        inadimplentes_data = dados_extraidos
                    elif tipo_dados == 'produtos_vida':
                        produtos_vida_data = dados_extraidos
                    elif tipo_dados == 'produtos_previdencia':
                        produtos_previdencia_data = dados_extraidos
            
            id_cpf_map = {}
            if clientes_data:
                logging.info("Criando mapa de ID do cliente para CPF...")
                for cliente in clientes_data:
                    id_cliente = cliente.get('id_cliente')
                    documento = cliente.get('documento')
                    if id_cliente and documento:
                        id_cpf_map[id_cliente] = clean_cpf(documento)
                logging.info(f"Mapa criado com {len(id_cpf_map)} entradas.")
            
            broker_id = None
            if clientes_data:
                broker_id = salvar_clientes_no_banco(clientes_data, corretora_nome, DB_URL)
            
            if not broker_id and corretora_nome != "NOME_DESCONHECIDO":
                logging.warning(f"Não foi possível obter o Broker ID a partir da seção de clientes. Tentando buscar pelo nome '{corretora_nome}'...")
                conn_temp = get_db_connection()
                if conn_temp:
                    try:
                        cur_temp = conn_temp.cursor()
                        cur_temp.execute("SELECT id FROM public.brokers WHERE UPPER(nome_completo) LIKE UPPER(%s)", (f"{corretora_nome}%",))
                        broker_row = cur_temp.fetchone()
                        if broker_row:
                            broker_id = broker_row[0]
                            logging.info(f"Broker ID para '{corretora_nome}' encontrado: {broker_id}.")
                        else:
                             logging.error(f"Broker com nome similar a '{corretora_nome}' não encontrado no banco de dados.")
                    finally:
                        conn_temp.close()

            if broker_id:
                if propostas_data:
                    salvar_propostas_no_banco(propostas_data, broker_id, id_cpf_map, DB_URL)
                if inadimplentes_data:
                    salvar_inadimplentes_no_banco(inadimplentes_data, corretora_nome, id_cpf_map, DB_URL)
                if produtos_vida_data:
                    salvar_produtos_vida_no_banco(produtos_vida_data, broker_id, corretora_nome, id_cpf_map, DB_URL)
                if produtos_previdencia_data:
                    salvar_produtos_previdencia_no_banco(produtos_previdencia_data, broker_id, corretora_nome, id_cpf_map, DB_URL)
            else:
                logging.error(f"Broker ID não obtido para '{corretora_nome}'. Sincronização pulada.")

            os.rename(caminho_arquivo, os.path.join(PASTA_PROCESSADOS, filename))
            logging.info(f"Arquivo '{filename}' processado e movido.")
        except Exception as e:
            logging.error(f"Ocorreu um erro grave ao processar o conteúdo do arquivo '{filename}': {e}")
            import traceback
            logging.error(traceback.format_exc())
            
    logging.info("\n--- SINCRONIZAÇÃO CONCLUÍDA ---")

if __name__ == "__main__":
    main()