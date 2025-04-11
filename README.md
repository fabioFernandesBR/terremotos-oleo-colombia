# MVP Sprint - Engenharia de Dados - PUC RIO
# Pipeline de Dados para Análise de ocorrência de terremotos e seus impactos na infraestrutura de transporte de óleo na Colômbia.
**Autor: Fábio Fernandes**

## Contexto
A Colômbia é um país da América do Sul localizado sobre a cadeia dos Andes, uma região montanhosa geologicamente instável, com elevada ocorrência de eventos sísmicos.

Ao mesmo tempo, é um importante produtor e consumidor de petróleo, gás e derivados de petróleo, com uma extensa rede de oleodutos e gasodutos para transporte deste produtos, além de terminais de transporte e tanques de estocagem. Como destaque, o Oleoducto Central é um oleoduto de petróleo bruto, que começa em Cusiana, região produtora, e vai até o terminal marítimo de Coveñas, no Caribe.
<img src="./imagens/Oleoducto_Central.png">
_Ver mais em:_ <a href="https://en-m-wikipedia-org.translate.goog/wiki/Ocensa_pipeline?_x_tr_sl=en&_x_tr_tl=pt&_x_tr_hl=pt&_x_tr_pto=sge#:~:text=O%20oleoduto%20Ocensa%20(%20Oleoducto%20Central,SA%20%2C%20Petrominerales%20e%20Triton%20Colombia)">Wikipedia do Oleoducto Central.</a>



Sabemos que os eventos sísmicos representam um perigo importante para a integridade desta malha logística, especialmente os dutos. De fato, terremotos podem provocar deslizamentos de terra. Caso haja algum duto instalado na área afetada pelo terremoto, diferentes anomalias podem surgir, como amassamentos ou mossas, enrugamento e até mesmo trincas ou rupturas decorrentes dos esforços mecânicos aplicados ao duto. Há muito esforço de pesquisa e desenvolvimento e aplicação de tecnologia para proteger a infraestrutura de transporte de óleo e gás desses riscos geotécnicos.
<a href="https://asmedigitalcollection.asme.org/ipg">Clique aqui para saber mais.</a>





## Objetivos deste trabalho
Para responder a algumas perguntas relacionadas à ocorrência de terremotos na Colômbia e potenciais impactos na infraestrutura de transporte de óleo, especificamente nas estações que fazem parte do Oleoducto Central, criamos um _pipeline_ de dados que nos permitiu fazer as consultas necessárias. 

**Os objetivos deste trabalho são documentar a criação deste _pipeline_, refletir sobre os aspectos mais relevantes das escolhas que foram feitas e sugerir caminhos para melhorias.**

### Perguntas a serem respondidas
- Qual a frequência de ocorrências de terremotos com magnitude maior do que 5 na Colômbia?
- Qual a média e o desvio padrão da magnitude destes terremotos?
- Com que frequência um evento sísmico de magnitude maior ou igual a 5 ocorre a menos de 200km de alguma das estações do Oleoducto Central?
- Qual das estações do Oleoducto Central foi mais vezes impactada por terremotos de magnitude maior do que 5?

### Estratégia de Implementação
Este _pipeline_ de dados foi implementado na plataforma _Databricks Community Edition_, onde, além dos recursos da plataforma, foram criados e executados algoritmos em Python, organizados nos _notebooks_. Esses algoritmos são responsáveis por obter dados, organizá-los, transformá-los e finalmente carregá-los em um esquema SQL. Da obtenção dos dados à consulta, utilizamos a arquitetura _medallion_, onde na camada bronze armazenamos os dados brutos; na camada prata, os dados limpos e transformados; e na camada ouro, os dados prontos para consulta.
(_Ver mais sobre medallion architecture aqui: https://www.databricks.com/glossary/medallion-architecture_.)

O produto final é um banco de dados SQL que pode ser utilizado para responder às perguntas, por meio de consultas SQL.

## Pipeline de dados
A imagem a seguir ilustra o pipeline que foi implementado
<img src="./imagens/Processo_ETL_medallion.png" alt="Logo">


Duas fontes de dados foram utilizadas: uma com dados básicos sobre as estações do Oleoducto Central, e outra com informações sobre terremotos. Mais adiante discutiremos sobre estas fontes.
A ETL foi construída por um total de 6 _notebooks_, sendo 2 na camada bronze, para leitura e armazenamento dos dados brutos das duas fontes de dados, três na camada prata, sendo 2 para tratamentos dos dados brutos e 1 para combinação dos 2 datasets em um terceiro dataset, e finalmente 1 _notebook_ na camada ouro para criar o esquema SQL e mover os 3 datasets da camada prata para as respectivas tabelas do esquema SQL.

Também foi criado 1 _notebook_ para consulta SQL.

### Esquema do banco de dados
O esquema adotado segue um modelo estrela, com 1 tabela-fato central conectada a 2 tabelas auxiliares. A tabela-fato foi nomeada _impactos_ e relaciona eventos sísmicos (terremotos) a instalações do Oleoducto Central, informando também a distância calculada entre o epicentro do terremoto e a localização da instalação. As tabelas auxiliares foram nomeadas _terremotos_ e _instalacoes_, e trazem mais detalhes respectivamente sobre os terremotos e sobre as instalações. As variáveis _id_ e _codigo_ funcionam como chaves primárias nas respectivas tabelas e como chave estrangeira na tabela _impactos_. 

<img src="./imagens/EsquemaSQL.png" alt="Logo">

_Observação: o Databricks Community Edition não permite a criação de esquemas SQL com integridade referencial, ou seja, o uso de chaves primárias e estrangeiras. Essa funcionalidade está disponível na versão paga. No entanto com comandos SQL é possível fazer pesquisa em uma tabela considerando informações em outra tabela. Na camada prata é feita uma checagem da variável id, garantindo que não há registros com variável id nulos, em branco ou duplicados, portanto funcionando como chave primária para a tabela terremotos. Basta uma inspeção visual nos dados das instalações para se assegurar de que a variável codigo também pode ser usada como chave primária na tabela onde constam os dados sobre as instalações._

### Catálogo de dados

#### Informações Gerais
- **Nome do Conjunto de Dados:** Impactos de Terremotos em Instalações do Oleoducto Central.
- **Descrição:** Dados relacionados aos impactos de terremotos nas instalações do Oleoducto Central.
- **Proprietário:** Fábio Fernandes
- **Data de Criação:** 08/04/2025
- **Última Atualização:** 10/04/2025

#### Metadados
- **Fonte dos Dados:** USGS (United States Geological Survey - https://www.usgs.gov/), webpage da Ocensa (https://www.ocensa.com.co/nosotros/historia.html) e Google Maps (https://www.google.com/maps/).
- **Formato dos Dados:** SQL
- **Frequência de Atualização:** Não definida
- **Tags:** terremotos, impactos, instalações, desastres naturais, oleodutos

#### Estrutura dos Dados

##### Tabela: impactos
- **Nome da Coluna:** instalacoes_cod
  - **Tipo de Dados:** VARCHAR(8)
  - **Descrição:** Identificador da instalação (chave estrangeira)
  - **Exemplo de Valor:** COV

- **Nome da Coluna:** terremotos_id
  - **Tipo de Dados:** VARCHAR(255)
  - **Descrição:** Identificador do terremoto (chave estrangeira)
  - **Exemplo de Valor:** at00sthakw

- **Nome da Coluna:** distancia
  - **Tipo de Dados:** DOUBLE
  - **Descrição:** Distância calculada entre o epicentro do terremoto (coordenadas latitude e longitude) e as coordenadas latitude e longitude da instalação.
  - **Exemplo de Valor:** 202

##### Tabela: terremotos
- **Nome da Coluna:** id
  - **Tipo de Dados:** VARCHAR(255)
  - **Descrição:** Identificador único do terremoto
  - **Exemplo de Valor:** at00sthakw

- **Nome da Coluna:** data
  - **Tipo de Dados:** Date
  - **Descrição:** Data de ocorrência do terremoto
  - **Exemplo de Valor:** 2025-03-15

- **Nome da Coluna:** prop_mag
  - **Tipo de Dados:** Double
  - **Descrição:** Magnitude do terremoto
  - **Exemplo de Valor:** 7.8

- **Nome da Coluna:** prop_url
  - **Tipo de Dados:** VARCHAR(1024)
  - **Descrição:** URL para o cadastro do terremoto no site do USGS, onde o usuário poderá buscar informações mais detalhadas sobre o evento.
  - **Exemplo de Valor:** https://earthquake.usgs.gov/earthquakes/eventpage/at00sthakw/executive
 
- **Nome da Coluna:** latitude
  - **Tipo de Dados:** DOUBLE
  - **Descrição:** latitude da ocorrência do terremoto
  - **Exemplo de Valor:** 7.2
 
- **Nome da Coluna:** longitude
  - **Tipo de Dados:** DOUBLE
  - **Descrição:** longitude da ocorrência do terremoto
  - **Exemplo de Valor:** -82.3
 
- **Nome da Coluna:** pais
  - **Tipo de Dados:** VARCHAR(255)
  - **Descrição:** país onde o terremoto se originou
  - **Exemplos de Valor:** Desconhecido, Colombia, Venezuela

##### Tabela: instalacoes
- **Nome da Coluna:** codigo
  - **Tipo de Dados:** VARCHAR(8)
  - **Descrição:** Código identificador único da instalação
  - **Exemplo de Valor:** COV

- **Nome da Coluna:** instalacao
  - **Tipo de Dados:** VARCHAR(255)
  - **Descrição:** Nome da instalação
  - **Exemplo de Valor:** Terminal Coveñas

- **Nome da Coluna:** operador
  - **Tipo de Dados:** VARCHAR(255) 
  - **Descrição:** Empresa que opera a instalação
  - **Exemplo de Valor:** Ocensa
 
- **Nome da Coluna:** latitude_instalacao
  - **Tipo de Dados:** DOUBLE
  - **Descrição:** latitude da localização da instalação
  - **Exemplo de Valor:** 9.409136280745889
 
- **Nome da Coluna:** longitude_instalacao
  - **Tipo de Dados:** DOUBLE
  - **Descrição:** longitude da localização da instalação
  - **Exemplo de Valor:** -75.70015297027714

#### Qualidade dos Dados
- **Completude:** 100% dos dados sobre as instalações que compõem o Oleoducto Central, 100% dos dados sobre os terremotos com magnitude >= 5 cuja ocorrência tem epicentro em um raio de até 1000km de um ponto central da Colômbia.
**A qualidade dos dados é altamente dependente da qualidade dos dados disponibilizados pela USGS por meio de sua API.**

#### Governança
- **Políticas de Acesso:** A base de dados SQL está disponível apenas na minha conta no Databricks Community Edition, porém o arquivo csv com informações sobre as instalações está disponível neste repositório (aqui) e nos _notebooks_ estão os códigos para leitura de dados, inclusive da API da USGS, todo o processamento das camadas bronze, prata e ouro, portanto é possível replicar a criação da base de dados SQL.
- **Regras de Privacidade:** Dados públicos.
- **Compliance:** Atende às normas da LGPD.

#### Histórico de Alterações
- **Data:** 10/04/2025
- **Alteração:** Nova execução dos _notebooks_, com potencial atualização da base de dados, em caso de ocorrência de algum terremoto que atenda aos critérios de busca estabelecidos.
- **Responsável:** Fábio Fernandes


## Busca pelos dados

2 fontes de dados foram usadas: API do USGS (https://earthquake.usgs.gov/fdsnws/event/1/query) e uma tabela em formato csv contendo a localização / coordenadas das instalações e a empresa responsável pela operação. Essa tabela csv foi construída manualmente pelo autor a partir de buscas no Google Maps. As buscas no Google Maps tiveram como base a relação de instalações do Oleoducto Central, disponível no site da Ocensa e na Wikipedia.

Visão do Terminal Coveñas no Google Maps:
<img src=".\imagens\googlemapscovenas.png">

Visão da Estação Cusiana no Google Maps:
<img src=".\imagens\googlemapscusiana.png">

O arquivo csv está disponível aqui: https://github.com/fabioFernandesBR/terremotos-oleo-colombia/blob/main/dados-csv/instalacoes_oleoducto_central.csv

## Coleta e armazenamento de dados
Como visto acima, o arquivo csv com dados das instalações foi gerado manualmente por meio de consultas ao Google Maps, e depois foi carregado para a plataforma Databricks, também manualmente. Neste link para o Youtube vemos o processo de carregamento: https://youtu.be/hEx-HmHy8Fw. O <a href="https://github.com/fabioFernandesBR/terremotos-oleo-colombia/blob/main/notebooks/Camada%20Bronze%20-%20Notebook%201%20-%20Dados%20das%20Instala%C3%A7%C3%B5es.ipynb">_notebook 1_ da camada bronze</a> executa a leitura deste arquivo csv já carregado no Databricks, o carrega para um _dataframe_ no padrão _PySpark_ e finalmente o salva em formato _Delta Lake_, para ser recuperado posteriormente na camada prata.

Já os dados dos terremotos foram obtidos por meio de consulta à API da USGS e salvos no Databricks em formato _Delta Lake_. Todo este processo foi executado a partir do <a href="https://github.com/fabioFernandesBR/terremotos-oleo-colombia/blob/main/notebooks/Camada%20Bronze%20-%20Notebook%202%20-%20Dados%20dos%20terremotos.ipynb">_notebook 2_ da camada bronze</a>.

## Transformação dos dados
A transformação dos dados acontece em 3 _notebooks_ da camada prata.

O <a href="https://github.com/fabioFernandesBR/terremotos-oleo-colombia/blob/main/notebooks/Camada%20Prata%20-%20Notebook%201%20-%20Transforma%C3%A7%C3%A3o%20dos%20dados%20das%20instala%C3%A7%C3%B5es.ipynb">_notebook 1_ da camada prata</a> é responsável pela leitura do arquivo _Delta Lake_ gerado pelo _notebook 1_ da camada bronze, pelo tratamento dos dados, e por fim em salvar os dados tratados em outro arquivo _Delta Lake_. O único tratamento aplicado aqui é a separação das coordenadas latitude e longitude, que estavam juntas em uma única coluna de vetores, em duas colunas distintas.

O <a href="https://github.com/fabioFernandesBR/terremotos-oleo-colombia/blob/main/notebooks/Camada%20Prata%20-%20Notebook%202%20-%20Transforma%C3%A7%C3%A3o%20dos%20dados%20dos%20terremotos.ipynb">_notebook 2_ da camada prata</a> é responsável pela leitura do arquivo _Delta Lake_ gerado pelo _notebook 2_ da camada bronze, pelo tratamento dos dados, e por fim em salvar os dados tratados em outro arquivo _Delta Lake_. Aqui são executados diversos tratamentos, tais como eliminação de colunas desnecessárias, mudança de nomes de colunas, checagem de valores nulos, em branco e duplicados, reorganização dos dados das coordenadas para colunas individuais e geocodificação. 
Vale destacar a execução da geocodificação, que neste caso foi a determinação do nome do país onde está o epicentro do terremoto, a partir das coordenadas geográficas latitude e longitude. A geocodificação foi realizada por meio da API Nominatim, que está incluída na biblioteca **geopy**.
A API Nominatim oferece consultas limitadas a 1 por segundo. Por este motivo foi incluída uma instrução no código python para pausar 1 segundo entre as consultas. Isso faz com o que código fique lento. Para aproximadamente 400 consultas, o código é executado em 6 a 7 minutos.
A biblioteca **geopy não está disponível por padrão no ambiente disponibilizado quando se cria um cluster Databricks**. É necessário instalar a biblioteca manualmente na configuração do cluster. Na seção [Adição da biblioteca geopy na configuração do cluster Databricks](#adição-da-biblioteca-geopy-na-configuração-do-cluster-databricks) mostramos como deve ser feita esta instalação.
Na seção #adição-da-biblioteca-geopy-na-configuração-do_cluster-databricks mostramos como deve ser feita esta instalação.

## Carga dos dados
Camada Ouro

## Análise
### Qualidade dos dados
### Respostas às perguntas
## Auto avaliação
### Sugestão de próximos passos





Links importantes:
Documentação da API do USGS:
query: https://earthquake.usgs.gov/fdsnws/event/1/
output: https://earthquake.usgs.gov/data/comcat/index.php

## Adição da biblioteca geopy na configuração do cluster Databricks
O processo para incluir a biblioteca **geopy** no cluster Databricks, o equivalente a executar um comando pip install geopy em um ambiente de desenvolvimento como o VS Code, é uma operação simples. As imagens a seguir mostram o passo a passo. É necessário realizar este procedimento para se executar dois dos _notebooks_ da camada prata.

**Passo 1:** Clicar no cluster para abrir a caixa de diálogo para configuração manual e clicar na aba **Libraries**.
<img src=".\imagens\configmanual.png">

**Passo 2:** Clicar em **Install new**.
<img src=".\imagens\installnew.png">

**Passo 3:** Selecionar a opção **PyPI** e escrever **geopy** no campo correspondente. Finalmente clicar em **Install**.
<img src=".\imagens\pypi.png">



