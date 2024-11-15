from cassandra.cluster import Cluster, Session
from datetime import datetime, timedelta
import numpy as np
import os
import random
import uuid

def abre_conexao() -> Session:
    # Conectar ao cluster Cassandra
    cluster = Cluster(os.getenv("CASSANDRA_CONTACT_POINTS", "localhost").split(','))
    session = cluster.connect()
    return session

def gerar_lancamento():
    clientes = [
        ('2b162060', '2b162060'), # id_cliente, id_conta
        ('2b16242a', '2b16242a'), # id_cliente, id_conta
        ('2b16256a', '2b16256a'), # id_cliente, id_conta
        ('2b16353c', '2b16353c'), # id_cliente, id_conta
        ('2b1636ae', '2b1636ae'), # id_cliente, id_conta
        ('2b16396a', '2b16396a'), # id_cliente, id_conta
        ('2b163bcc', '2b163bcc'), # id_cliente, id_conta

        ('2b162060', '007c2c1c'), # id_cliente, id_cartao
        ('2b162060', '897199aa'), # id_cliente, id_cartao
        ('2b16242a', 'a2fab366'), # id_cliente, id_cartao
        ('2b16256a', 'cd586ffb')  # id_cliente, id_cartao
    ]

    estabelecimentos = ['Salão de beleza', 'Posto de gasolina', 'Supermercado', 'Restaurante', 'Farmácia', 'Padaria', 
                        'Loja de roupas', 'Loja de eletrônicos', 'Loja de móveis', 'Loja de brinquedos']

    cliente = random.choice(clientes)
    id_cliente = cliente[0]
    data_hora = datetime.now() - timedelta(days=random.randint(0, 365))
    competencia = data_hora.strftime('%Y-%m')
    data_hora = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    id_produto = cliente[1]
    status = str(np.random.choice(['APROVADA', 'RECUSADA'], p=[0.95, 0.05]))
    tipo_operacao = str(np.random.choice(['DEBITO', 'CREDITO'], p=[0.999, 0.001]))
    id_lancamento = ''.join(random.choices('0123456789abcdef', k=8))
    estabelecimento = random.choice(estabelecimentos)
    valor = round(random.uniform(10.0, 1000.0), 2)
    codigo_autenticacao = str(uuid.uuid4())

    return {
        'id_cliente': id_cliente,
        'competencia': competencia,
        'data_hora': data_hora,
        'id_produto': id_produto,
        'status': status,
        'tipo_operacao': tipo_operacao,
        'id_lancamento': id_lancamento,
        'estabelecimento': estabelecimento,
        'valor': valor,
        'codigo_autenticacao': codigo_autenticacao
    }

def cria_keyspace(session: Session):
    # Elimina a keyspace infobarbank
    session.execute("DROP KEYSPACE IF EXISTS infobarbank;")
    print("Keyspace infobarbank eliminada")

    # Criar keyspace infobarbank
    replication_factor =os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
    print(f"Replication factor: {replication_factor}")
    command = f"""
        CREATE KEYSPACE IF NOT EXISTS infobarbank 
        WITH replication = {{   
        'class': 'NetworkTopologyStrategy', 
        'datacenter1': {replication_factor} }};"""
    
    print(f"Creating keyspace: {command}")

    session.execute(command)

    # Criar tabelas
    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.cliente (
        id_cliente text PRIMARY KEY,
        Codigo int,
        Nome_Completo text,
        CPF text,
        Email text,
        Telefone text,
        Data_Nascimento text,
        Salario text,
        Estado text
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.cliente_por_telefone (
        Telefone text,
        id_cliente text,
        Codigo int,
        Nome_Completo text,
        CPF text,
        Email text,
        Data_Nascimento text,
        Salario text,
        Estado text,
        PRIMARY KEY (Telefone, id_cliente)
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.cliente_por_email (
        Email text,
        id_cliente text,
        Codigo int,
        Nome_Completo text,
        CPF text,
        Telefone text,
        Data_Nascimento text,
        Salario text,
        Estado text,
        PRIMARY KEY (Email, id_cliente)
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.cliente_por_cpf (
        CPF text,
        id_cliente text,
        Codigo int,
        Nome_Completo text,
        Email text,
        Telefone text,
        Data_Nascimento text,
        Salario text,
        Estado text,
        PRIMARY KEY (CPF, id_cliente)
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.conta_corrente(
        id_cliente          TEXT,
        status              TEXT,
        id_conta            TEXT,
        apelido_cliente     TEXT,
        data_ultimo_acesso  DATE,
        saldo_disponivel    FLOAT,
        PRIMARY KEY((id_cliente), status, id_conta)
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.cartao_credito(
        id_cliente          TEXT,
        status              TEXT,
        id_cartao           TEXT,
        num_cartao          TEXT,
        cvv	                TEXT,
        data_validade       DATE,
        limite_total        FLOAT,
        limite_disponivel   FLOAT,
        PRIMARY KEY((id_cliente), status, id_cartao)
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.lancamentos_cartao_credito(
        id_cliente      TEXT,
        competencia     TEXT,
        data_hora       TIMESTAMP,
        id_produto      TEXT,
        status          TEXT,
        tipo_operacao   TEXT,
        id_lancamento   TEXT,
        estabelecimento TEXT,
        valor           FLOAT,
        PRIMARY KEY((id_cliente, competencia), data_hora, id_produto, status, tipo_operacao, id_lancamento)
    ) WITH CLUSTERING ORDER BY (data_hora DESC, id_produto ASC, status ASC, tipo_operacao ASC, id_lancamento ASC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.lancamentos_por_status(
        id_cliente      TEXT,
        competencia     TEXT,
        data_hora       TIMESTAMP,
        id_produto      TEXT,
        status          TEXT,
        tipo_operacao   TEXT,
        id_lancamento   TEXT,
        estabelecimento TEXT,
        valor           FLOAT,
        PRIMARY KEY((id_cliente, competencia), status, data_hora, id_lancamento)
    ) WITH CLUSTERING ORDER BY (status ASC, data_hora DESC, id_lancamento ASC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.lancamentos_por_periodo(
        id_cliente      TEXT,
        competencia     TEXT,
        data_hora       TIMESTAMP,
        id_produto      TEXT,
        status          TEXT,
        tipo_operacao   TEXT,
        id_lancamento   TEXT,
        estabelecimento TEXT,
        valor           FLOAT,
        PRIMARY KEY((id_cliente, competencia), data_hora, id_lancamento)
    ) WITH CLUSTERING ORDER BY (data_hora DESC, id_lancamento ASC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.lancamentos_por_produto(
        id_cliente      TEXT,
        competencia     TEXT,
        data_hora       TIMESTAMP,
        id_produto      TEXT,
        status          TEXT,
        tipo_operacao   TEXT,
        id_lancamento   TEXT,
        estabelecimento TEXT,
        valor           FLOAT,
        PRIMARY KEY((id_cliente, competencia), id_produto, data_hora, id_lancamento)
    ) WITH CLUSTERING ORDER BY (id_produto ASC, data_hora DESC, id_lancamento ASC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.lancamentos_por_tipo(
        id_cliente      TEXT,
        competencia     TEXT,
        data_hora       TIMESTAMP,
        id_produto      TEXT,
        status          TEXT,
        tipo_operacao   TEXT,
        id_lancamento   TEXT,
        estabelecimento TEXT,
        valor           FLOAT,
        PRIMARY KEY((id_cliente, competencia), tipo_operacao, data_hora, id_lancamento)
    ) WITH CLUSTERING ORDER BY (tipo_operacao ASC, data_hora DESC, id_lancamento ASC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS infobarbank.lancamentos_detalhe(
        id_cliente          TEXT,
        competencia         TEXT,
        data_hora           TIMESTAMP,
        id_produto          TEXT,
        status              TEXT,
        tipo_operacao       TEXT,
        id_lancamento       TEXT,
        estabelecimento     TEXT,
        valor               FLOAT,
        codigo_autenticacao TEXT,
        PRIMARY KEY(id_lancamento)
    );
    """)

def popula_keyspace( session: Session):
    # Inserir registros nas tabelas
    clientes = [
        ('2b162060', 1, 'MARIVALDA KANAMARY', '987.539.360-60', 'marivalda@bmail.com', '(11) 987654321', '06/02/1997', 'R$5.000,00', 'SP'),
        ('2b16242a', 2, 'JUCILENE MOREIRA CRUZ', '124.554.260-50', 'jucilene@bmail.com', '(21) 945678900', '06/02/1944', 'R$2.500,00', 'RJ'),
        ('2b16256a', 3, 'GRACIMAR BRASIL GUERRA', '324.873.000-51', 'gracimar@bmail.com', '(21) 934455667', '03/08/1946', 'R$10.000,00', 'RJ'),
        ('2b16353c', 4, 'ALDENORA VIANA MOREIRA', '598.131.330-74', 'aldenora@gmail.com', '(11) 999775533', '04/07/1970', 'R$3.000,00', 'SP'),
        ('2b1636ae', 5, 'VERA LUCIA RODRIGUES SENA', '797.399.520-03', 'vera@bmail.com', '(31) 987987988', '01/06/2004', 'R$2.500,00', 'MG'),
        ('2b16396a', 6, 'IVONE GLAUCIA VIANA DUTRA', '661.428.060-00', 'ivone@bmail.com', '(11) 934877563', '13/01/1982', 'R$4.000,00', 'SP'),
        ('2b163bcc', 7, 'LUCILIA ROSA LIMA PEREIRA', '190.523.300-00', 'lucilia@yahoo.com', '(31) 911277444', '19/07/1954', 'R$20.000,00', 'MG'),
        ('2b163d1e', 8, 'RAIMUNDA PEREIRA LIMA', '432.098.160-90', 'raimunda@bmail.com', '(61) 999887766', '23/09/1965', 'R$7.000,00', 'DF'),
        ('2b163f70', 9, 'MARIA DO SOCORRO SILVA', '129.384.250-60', 'mariasocorro@bmail.com', '(11) 965431221', '30/01/1990', 'R$3.500,00', 'SP'),
        ('2b1641c2', 10, 'JOÃO CARLOS SILVA', '839.239.480-71', 'joao@bmail.com', '(21) 954321223', '12/12/1983', 'R$5.500,00', 'RJ'),
        ('2b164414', 11, 'ANA PAULA COSTA', '582.321.910-23', 'ana@bmail.com', '(41) 963212312', '05/06/1995', 'R$6.200,00', 'PR'),
        ('2b164666', 12, 'FRANCISCO OLIVEIRA LIMA', '923.984.230-12', 'francisco@bmail.com', '(81) 984532112', '17/03/1972', 'R$4.800,00', 'PE'),
        ('2b1648b8', 13, 'CLAUDIA APARECIDA DOS SANTOS', '102.394.870-34', 'claudia@bmail.com', '(31) 978787878', '08/11/1985', 'R$3.700,00', 'MG'),
        ('2b164b0a', 14, 'LUCAS GOMES SILVA', '829.482.650-45', 'lucas@bmail.com', '(71) 923456712', '11/08/1993', 'R$4.900,00', 'BA'),
        ('2b164d5c', 15, 'PAULO CESAR FERREIRA', '294.829.470-56', 'paulo@bmail.com', '(51) 954321223', '22/05/1987', 'R$5.300,00', 'RS'),
        ('2b164fae', 16, 'ROSANA MARIA TEIXEIRA', '594.238.920-67', 'rosana@bmail.com', '(21) 963212123', '03/02/1981', 'R$6.000,00', 'RJ'),
        ('2b165200', 17, 'MARCOS ANTONIO DA SILVA', '192.834.780-78', 'marcos@bmail.com', '(11) 944556677', '19/12/1969', 'R$9.500,00', 'SP'),
        ('2b165452', 18, 'RENATA ALMEIDA GONÇALVES', '219.384.290-89', 'renata@bmail.com', '(31) 966554433', '02/09/1974', 'R$4.200,00', 'MG'),
        ('2b1656a4', 19, 'ANTONIO JOSÉ PEREIRA', '203.947.560-90', 'antonio@bmail.com', '(85) 955667788', '25/11/1966', 'R$5.800,00', 'CE'),
        ('2b1658f6', 20, 'FABIANA SILVA MENEZES', '875.392.340-91', 'fabiana@bmail.com', '(61) 999667788', '07/07/1975', 'R$7.200,00', 'DF'),
        ('2b165b48', 21, 'CARLOS EDUARDO MARTINS', '892.394.120-12', 'carlos@bmail.com', '(21) 965432211', '21/01/1991', 'R$4.600,00', 'RJ'),
        ('2b165d9a', 22, 'EDSON LUIZ PEREIRA', '204.839.470-23', 'edson@bmail.com', '(11) 977665544', '29/10/1988', 'R$5.700,00', 'SP'),
        ('2b165fec', 23, 'JOANA FERREIRA DOS SANTOS', '198.234.780-34', 'joana@bmail.com', '(41) 968756434', '05/06/1992', 'R$4.500,00', 'PR'),
        ('2b16623e', 24, 'ADRIANA MOURA', '320.984.230-45', 'adriana@bmail.com', '(71) 932156712', '15/08/1986', 'R$4.300,00', 'BA'),
        ('2b166490', 25, 'RICARDO GOMES PEREIRA', '198.472.650-56', 'ricardo@bmail.com', '(51) 933456712', '27/05/1995', 'R$5.600,00', 'RS'),
        ('2b1666e2', 26, 'JULIANA MENDES DA SILVA', '592.238.920-67', 'juliana@bmail.com', '(21) 962232123', '08/02/1983', 'R$5.000,00', 'RJ'),
        ('2b166934', 27, 'MARCELO TEIXEIRA LIMA', '103.839.470-78', 'marcelo@bmail.com', '(11) 944576677', '12/12/1979', 'R$8.900,00', 'SP'),
        ('2b166b86', 28, 'FERNANDA LOPES PEREIRA', '204.938.560-89', 'fernanda@bmail.com', '(31) 966564433', '16/09/1990', 'R$4.700,00', 'MG'),
        ('2b166dd8', 29, 'JOÃO CARLOS DA SILVA', '212.947.560-90', 'joaocarlos@bmail.com', '(85) 953567788', '25/03/1984', 'R$6.800,00', 'CE'),
        ('2b16702a', 30, 'ELIZABETH SOUZA SILVA', '875.392.340-91', 'elizabeth@bmail.com', '(61) 999667899', '18/08/1987', 'R$7.500,00', 'DF'),
        ('2b16727c', 31, 'ROBERTO CARLOS MARTINS', '892.394.120-12', 'roberto@bmail.com', '(21) 964432211', '13/02/1992', 'R$4.900,00', 'RJ'),
        ('2b1674ce', 32, 'SANDRA REGINA PEREIRA', '204.839.470-23', 'sandra@bmail.com', '(11) 975665544', '29/11/1980', 'R$5.200,00', 'SP'),
        ('2b167720', 33, 'JOSÉ ANTONIO DOS SANTOS', '195.234.780-34', 'joseantonio@bmail.com', '(41) 968756734', '15/06/1969', 'R$9.200,00', 'PR'),
        ('2b167972', 34, 'MONICA TEIXEIRA', '320.984.230-45', 'monica@bmail.com', '(71) 922156712', '21/07/1972', 'R$6.000,00', 'BA'),
        ('2b167bc4', 35, 'ANDREIA GOMES PEREIRA', '198.472.650-56', 'andreia@bmail.com', '(51) 933456733', '11/05/1994', 'R$5.100,00', 'RS'),
        ('2b167e16', 36, 'FABIO MENDES DA SILVA', '592.238.920-67', 'fabio@bmail.com', '(21) 964232124', '12/02/1979', 'R$6.300,00', 'RJ'),
        ('2b168068', 37, 'MARCIA TEIXEIRA LIMA', '103.839.470-78', 'marcia@bmail.com', '(11) 944576778', '10/12/1982', 'R$8.100,00', 'SP'),
        ('2b1682ba', 38, 'ANA LUCIA LOPES PEREIRA', '204.938.560-89', 'analucia@bmail.com', '(31) 966564488', '15/09/1977', 'R$5.400,00', 'MG'),
        ('2b16850c', 39, 'PEDRO HENRIQUE DA SILVA', '212.947.560-90', 'pedrohenrique@bmail.com', '(85) 953567789', '18/04/1983', 'R$7.300,00', 'CE'),
        ('2b16875e', 40, 'CAMILA SOUZA SILVA', '875.392.340-91', 'camila@bmail.com', '(61) 999667788', '23/05/1995', 'R$6.800,00', 'DF'),
        ('2b1689b0', 41, 'MARIA EDUARDA MARTINS', '892.394.120-12', 'mariaeduarda@bmail.com', '(21) 962432221', '12/01/1997', 'R$4.600,00', 'RJ'),
        ('2b168c02', 42, 'RODRIGO CARLOS MARTINS', '892.394.120-12', 'rodrigo@bmail.com', '(21) 964433321', '18/06/1993', 'R$5.400,00', 'RJ'),
        ('2b168e54', 43, 'LUANA REGINA PEREIRA', '204.839.470-23', 'luana@bmail.com', '(11) 974665544', '22/09/1986', 'R$5.700,00', 'SP'),
        ('2b1690a6', 44, 'MARIA FERNANDA DOS SANTOS', '195.234.780-34', 'mariafernanda@bmail.com', '(41) 969756734', '19/04/1991', 'R$4.800,00', 'PR'),
        ('2b1692f8', 45, 'GUSTAVO TEIXEIRA', '320.984.230-45', 'gustavo@bmail.com', '(71) 922456712', '25/05/1987', 'R$6.300,00', 'BA'),
        ('2b16954a', 46, 'PAULA GOMES PEREIRA', '198.472.650-56', 'paula@bmail.com', '(51) 933456732', '18/05/1992', 'R$5.900,00', 'RS'),
        ('2b16979c', 47, 'CAROLINA MENDES DA SILVA', '592.238.920-67', 'carolina@bmail.com', '(21) 963332124', '21/02/1993', 'R$6.100,00', 'RJ'),
        ('2b1699ee', 48, 'CLAUDIO TEIXEIRA LIMA', '103.839.470-78', 'claudio@bmail.com', '(11) 944476677', '24/12/1982', 'R$7.100,00', 'SP'),
        ('2b169c40', 49, 'FABIANO LOPES PEREIRA', '204.938.560-89', 'fabiano@bmail.com', '(31) 966564477', '28/09/1980', 'R$5.600,00', 'MG'),
        ('2b169e92', 50, 'JULIO CESAR DA SILVA', '212.947.560-90', 'juliocesar@bmail.com', '(85) 953567787', '31/03/1988', 'R$6.500,00', 'CE')
    ]

    for cliente in clientes:
        session.execute("""
        INSERT INTO infobarbank.cliente (id_cliente, Codigo, Nome_Completo, CPF, Email, Telefone, Data_Nascimento, Salario, Estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cliente)

        session.execute("""
        INSERT INTO infobarbank.cliente_por_telefone (Telefone, id_cliente, Codigo, Nome_Completo, CPF, Email, Data_Nascimento, Salario, Estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (cliente[5], cliente[0], cliente[1], cliente[2], cliente[3], cliente[4], cliente[6], cliente[7], cliente[8]))

        session.execute("""
        INSERT INTO infobarbank.cliente_por_email (Email, id_cliente, Codigo, Nome_Completo, CPF, Telefone, Data_Nascimento, Salario, Estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (cliente[4], cliente[0], cliente[1], cliente[2], cliente[3], cliente[5], cliente[6], cliente[7], cliente[8]))

        session.execute("""
        INSERT INTO infobarbank.cliente_por_cpf (CPF, id_cliente, Codigo, Nome_Completo, Email, Telefone, Data_Nascimento, Salario, Estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (cliente[3], cliente[0], cliente[1], cliente[2], cliente[4], cliente[5], cliente[6], cliente[7], cliente[8]))

    contas_correntes = [
        ('2b162060', 'ATIVA' , '2b162060', 'MARIVALDA', '2024-09-01', 100.00),
        ('2b16242a', 'ATIVA' , '2b16242a', 'JUCILENE' , '2024-08-01', 500.00),
        ('2b16256a', 'ATIVA' , '2b16256a', 'GRACIMAR' , '2024-09-04', 30.00),
        ('2b16353c', 'ATIVA' , '2b16353c', 'ALDENORA' , '2024-09-03', 1500.00),
        ('2b1636ae', 'ATIVA' , '2b1636ae', 'VERA'     , '2024-09-02', 70.00),
        ('2b16396a', 'ATIVA' , '2b16396a', 'IVONE'    , '2024-08-30', 400.00),
        ('2b163bcc', 'ATIVA' , '2b163bcc', 'LUCILIA'  , '2024-08-31', 350.00)
    ]

    for conta in contas_correntes:
        session.execute("""
        INSERT INTO infobarbank.conta_corrente (id_cliente, status, id_conta, apelido_cliente, data_ultimo_acesso, saldo_disponivel)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, conta)

    cartoes_credito = [
        ('2b162060', 'ATIVO', '007c2c1c', '5450 3799 9172 8454', '423', '2030-02-21', 1000.00, 900.00),
        ('2b162060', 'ATIVO', '897199aa', '5576 8924 9861 7490', '336', '2030-07-21', 200.00, 50.00),
        ('2b162060', 'INATIVO', '9a748c3c', '5188 2716 2053 8850', '920', '2024-03-21', 500.00, 100.00),
        ('2b16242a', 'ATIVO', 'a2fab366', '5590 5738 5562 1778', '198', '2024-03-21', 5000.00, 4000.00),
        ('2b16256a', 'ATIVO', 'cd586ffb', '5412 2646 1222 3738', '273', '2030-02-21', 3000.00, 1500.00)
    ]

    for cartao in cartoes_credito:
        session.execute("""
        INSERT INTO infobarbank.cartao_credito (id_cliente, status, id_cartao, num_cartao, cvv, data_validade, limite_total, limite_disponivel)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, cartao)


    # Lancamentos de cartão de crédito
    for _ in range(1000):

        l = gerar_lancamento()
        lancamento = (l['id_cliente'], l['competencia'], l['data_hora'], l['id_produto'], l['status'], l['tipo_operacao'], l['id_lancamento'], l['estabelecimento'], l['valor'])
        print(lancamento)

        session.execute("""
        INSERT INTO infobarbank.lancamentos_cartao_credito (id_cliente, competencia, data_hora, id_produto, status, tipo_operacao, id_lancamento, estabelecimento, valor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lancamento)

        session.execute("""
        INSERT INTO infobarbank.lancamentos_por_status (id_cliente, competencia, data_hora, id_produto, status, tipo_operacao, id_lancamento, estabelecimento, valor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lancamento)

        session.execute("""
        INSERT INTO infobarbank.lancamentos_por_periodo (id_cliente, competencia, data_hora, id_produto, status, tipo_operacao, id_lancamento, estabelecimento, valor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lancamento)

        session.execute("""
        INSERT INTO infobarbank.lancamentos_por_produto (id_cliente, competencia, data_hora, id_produto, status, tipo_operacao, id_lancamento, estabelecimento, valor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lancamento)

        session.execute("""
        INSERT INTO infobarbank.lancamentos_por_tipo (id_cliente, competencia, data_hora, id_produto, status, tipo_operacao, id_lancamento, estabelecimento, valor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lancamento)

        session.execute("""
        INSERT INTO infobarbank.lancamentos_detalhe (id_cliente, competencia, data_hora, id_produto, status, tipo_operacao, id_lancamento, estabelecimento, valor, codigo_autenticacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lancamento + (l['codigo_autenticacao'],))

def main():
    session = abre_conexao()
    cria_keyspace(session = session)
    popula_keyspace(session = session)

if __name__ == "__main__":
    main()
