Pipeline de Extração e Sincronização Icatu
Este projeto automatiza a coleta de dados do Portal do Corretor da Icatu Seguros. Ele utiliza uma combinação de RPA (Robotic Process Automation) com Playwright para extrair dados e um script ETL para carregar e atualizar um banco de dados PostgreSQL.

O sistema é resiliente, contando com um verificador de logs que identifica falhas na extração e gera uma lista para re-execução, garantindo a integridade e a completude dos dados.

Principais Funcionalidades
extrator_icatu.py (O Extrator)

Realiza login no portal Icatu usando credenciais de ambiente (ICATU_USUARIO, ICATU_SENHA).

Itera sobre uma lista de corretoras filhas a partir de um arquivo Excel (corretoras.xlsx ou corretoras_para_rerodar.xlsx).

Intercepta o token de autenticação da API interna do portal para consultas diretas e assíncronas.

Extrai dados de Clientes, Produtos (Vida e Previdência), Status de Propostas e Pagamentos Pendentes.

Salva os dados brutos em arquivos .xlsx (para análise) e .json (para o banco de dados) na pasta downloads/.

Gera um log detalhado de execução no arquivo execucoes.log.

verificador_log.py (O Verificador)

Analisa o execucoes.log para identificar quais corretoras foram processadas com a mensagem "Extração concluída para...".

Compara a lista de sucesso com a lista original de corretoras (corretoras.xlsx).

Gera um novo arquivo Excel (corretoras_para_rerodar.xlsx) contendo apenas as corretoras que falharam ou não foram processadas.

sincronizar_banco.py (O Sincronizador)

Lê todos os arquivos _backup.json da pasta downloads/.

Conecta-se a um banco de dados PostgreSQL usando uma URL de conexão (DB_URL) de ambiente.

Processa e limpa os dados (ex: CPFs, datas) antes da inserção.

Realiza operações "Upsert" (INSERT ou UPDATE) de forma inteligente, verificando a existência de registros antes de inserir ou atualizar.

Sincroniza os dados nas tabelas clients, proposals, defaulters_detailed e products_clients.

Move os arquivos .json processados para a pasta downloads/processados/ para evitar duplicidade na próxima execução.

Tecnologias Utilizadas
Python 3.x

Playwright (Async): Para a automação do navegador (RPA).

asyncio: Para gerenciamento de concorrência nas requisições de API.

httpx: Cliente HTTP assíncrono para as chamadas de API.

Pandas: Para manipulação dos arquivos Excel de entrada e saída.

Psycopg2: Driver para conexão com o banco de dados PostgreSQL.

Openpyxl: Para interação com os arquivos .xlsx.

Python-dotenv: Para gerenciamento de variáveis de ambiente.
