# Desafio Airflow Indicium

Desafio sobre o uso da ferramenta Airflow, no programa Lighthouse da Indicium Tech. 

## Instruções de execução

### 1. Abra o terminal, navegue até a pasta onde será clonado o repositório e digite:

```
git clone https://github.com/felipecampelo/desafio-airflow.git
```

### 2. Crie o ambiente virtual do Python:

```
virtualenv venv -p python3
source venv/bin/activate
```

### 3. Execute o arquivo install.bash:

```
bash install.sh
```

### 4. Instale as dependências do requirements.txt:

```
pip install -r requirements.txt
```

### 5. Entre no arquivo airflow.cfg que está dentro da pasta airflow-data e modifique a pasta de onde será armazenadas as dags do projeto:

```
# The folder where your airflow pipelines live, most likely a
# subfolder in a code repository. This path must be absolute.
dags_folder = YOUR_DAGS_FOLDER
```

### 6. Configure o ambiente para dizer onde vão ficar os arquivos de config do airflow:

```
export AIRFLOW_HOME=./airflow-data
```

### 7. Agora, é necessário resetar o db do airflow e depois podemos iniciá-lo.

```
airflow db reset
airflow standalone
```

Nesse momento, o airflow local irá iniciar e o ambiente poderá ser acessado via localhost:8080 no browser. Você poderá usar o login fornecido pelo Airflow no terminal, cujo user é admin.

### 8. Vá no menu Admin > Variables e adicione o seu e-mail em uma variável com chave "my_email" como na imagem abaixo:

![image](https://user-images.githubusercontent.com/13797593/236270667-7e61a445-a0a4-4ba1-b676-605cca807040.png)

### 9. Para executar a DAG, simplesmente apertamos o botão de play da mesma no ambiente:

![image](https://user-images.githubusercontent.com/13797593/236197826-6b75f078-0666-4483-ac41-963310701dee.png)

Ao finalizar a execução, serão criados os arquivos count.txt, output_orders.csv e o final_output.txt, que são as soluções do desafio.

## Código Explicado

### Task sqlite_to_CSV

Tem como função a extração da tabela Order do banco de dados sqlite e sua conversão para CSV, sendo armazenado no diretório raíz do projeto.

```python
def sqlite_read():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("airflow-data/data/Northwind_small.sqlite")

    # Creating the cursor
    cur = con.cursor()

    # Executing que SQL Query
    cur.execute('SELECT * FROM "Order";')

    with open("output_orders.csv", 'w', newline='') as csv_file: 
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cur.description]) 
        csv_writer.writerows(cur)

    print('CSV file created: output_orders.csv')
    con.close()
```
    
### Task query_result_txt

Tem como função extrair a tabela OrderDetail, fazer a leitura do arquivo CSV gerado pela task sqlite_to_CSV, realizar um JOIN entre as tabelas com comandos SQL, calcular o número de itens vendidos com o ShipCity do Rio de Janeiro e armazenar o resultado em um arquivo txt (count.txt) no diretório raíz.

```python
def query_result():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("airflow-data/data/Northwind_small.sqlite")

    # Creating Pandas DataFrame
    df_orderDetails = pd.read_sql_query("SELECT * FROM OrderDetail", con)
    print('Printando o df_orderDetails')

    # Reading the CSV file
    df_Order = pd.read_csv("output_orders.csv")
    print('Printando o df_Order')

    # Creating a query to join the DataFrames
    query = """
    SELECT 
        SUM(Quantity) 
    FROM 
        df_orderDetails
    JOIN 
        df_Order 
    ON 
        df_Order.Id = df_orderDetails.OrderId
    WHERE 
        ShipCity = 'Rio de Janeiro'
    GROUP BY 
        ShipCity;
    """

    # Saving the result in a Pandas DataFrame
    df_result = psql.sqldf(query)

    # Converting Pandas DataFrame to txt
    df_result.to_csv('count.txt', header=None, index=None, mode='w')
```

### Task export_final_output

Por fim, essa task faz a leitura do arquivo count.txt e o codifica, fazendo uso da variável do ambiente my_email. O texto codificado é armazenado em outro arquivo, chamado final_output.txt.

```python
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
```

## Sequenciamento da pipeline
```python
sqlite_to_CSV >> query_result_txt >> export_final_output
```
