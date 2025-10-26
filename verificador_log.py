# Nome do arquivo: verificador_log.py (versão corrigida)
import pandas as pd
import re
import os
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

# --- CONFIGURAÇÕES ---
ARQUIVO_LOG = 'execucoes.log'
ARQUIVO_CORRETORAS_ORIGINAL = 'corretoras.xlsx'
ARQUIVO_SAIDA_PENDENTES = 'corretoras_para_rerodar.xlsx'
NOME_CORRETORA_MAE = "OUTLIER CORRETORA LTDA"
# --------------------

def analisar_log_e_gerar_pendentes():
    """
    Lê o arquivo de log para encontrar corretoras processadas com sucesso
    e gera uma nova planilha com as que falharam ou não foram processadas.
    """
    if not os.path.exists(ARQUIVO_LOG):
        logging.error(f"Arquivo de log '{ARQUIVO_LOG}' não encontrado. Execute o extrator primeiro.")
        return

    if not os.path.exists(ARQUIVO_CORRETORAS_ORIGINAL):
        logging.error(f"Arquivo de corretoras original '{ARQUIVO_CORRETORAS_ORIGINAL}' não encontrado.")
        return

    logging.info(f"Analisando o arquivo de log: '{ARQUIVO_LOG}'...")

    corretoras_sucesso = set()
    
    # Expressão regular para encontrar as corretoras que foram concluídas com sucesso.
    # Exemplo da linha: "Extração concluída para NOME DA CORRETORA!"
    regex_sucesso = re.compile(r"Extração concluída para (.*)!")

    # <<< INÍCIO DA CORREÇÃO >>>
    # Alterado o encoding para 'cp1252' para ser compatível com o padrão do Windows
    with open(ARQUIVO_LOG, 'r', encoding='cp1252') as f:
    # <<< FIM DA CORREÇÃO >>>
        for linha in f:
            match = regex_sucesso.search(linha)
            if match:
                # O grupo 1 contém o nome da corretora capturado
                nome_corretora = match.group(1).strip()
                corretoras_sucesso.add(nome_corretora)

    if not corretoras_sucesso:
        logging.warning("Nenhuma corretora foi concluída com sucesso no último log.")
    else:
        logging.info(f"Encontradas {len(corretoras_sucesso)} corretoras concluídas com sucesso.")

    # Carrega a lista original de todas as corretoras
    df_original = pd.read_excel(ARQUIVO_CORRETORAS_ORIGINAL)
    
    # Remove a corretora mãe da lista, caso ela exista
    df_original = df_original[df_original['nome'].str.strip().str.lower() != NOME_CORRETORA_MAE.lower()]
    
    total_corretoras = len(df_original)
    logging.info(f"Total de corretoras na lista original (filhas): {total_corretoras}")

    # Filtra o DataFrame, mantendo apenas as corretoras que NÃO estão na lista de sucesso
    lista_sucesso = list(corretoras_sucesso)
    df_pendentes = df_original[~df_original['nome'].isin(lista_sucesso)]

    num_pendentes = len(df_pendentes)
    
    if num_pendentes == 0:
        logging.info("PARABÉNS! Todas as corretoras foram processadas com sucesso. Nenhuma ação é necessária.")
        if os.path.exists(ARQUIVO_SAIDA_PENDENTES):
            os.remove(ARQUIVO_SAIDA_PENDENTES)
            logging.info(f"Arquivo antigo '{ARQUIVO_SAIDA_PENDENTES}' removido.")
    else:
        logging.warning(f"Total de {num_pendentes} corretoras pendentes (falharam ou não foram processadas).")
        logging.info(f"Gerando a lista de re-execução em '{ARQUIVO_SAIDA_PENDENTES}'...")
        
        # Salva a nova lista em um arquivo Excel
        df_pendentes.to_excel(ARQUIVO_SAIDA_PENDENTES, index=False)
        
        print("\n--- RESUMO ---")
        print(f"Total de Corretoras: {total_corretoras}")
        print(f"Sucesso: {len(corretoras_sucesso)}")
        print(f"Pendentes: {num_pendentes}")
        print(f"\nPróximo Passo: Altere a variável 'arquivo_corretoras' no script 'extrator_icatu.py' para '{ARQUIVO_SAIDA_PENDENTES}' e execute-o novamente.")

if __name__ == "__main__":
    analisar_log_e_gerar_pendentes()