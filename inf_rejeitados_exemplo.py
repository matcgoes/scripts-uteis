import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

# Leitura da base de dados
data = pd.read_csv('base_de_dados.csv')

# Definição das variáveis explicativas (X) e da variável resposta (y)
X = data[['idade', 'sexo', 'renda', 'historico_compras']]
y = data['contratou_x']

# Divisão da base de dados em treino e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

# Treinamento do modelo de regressão logística
model = LogisticRegression()
model.fit(X_train, y_train)

# Aplicação do modelo aos clientes que não contrataram o produto X
X_nao_contrataram = data[data['contratou_x'] == 0][['idade', 'sexo', 'renda', 'historico_compras']]
probabilidade_contratacao = model.predict_proba(X_nao_contrataram)

# Análise das diferenças de médias
media_gastos_contratantes = data[data['contratou_x'] == 1]['gastos'].mean()
media_gastos_nao_contratantes = data[data['contratou_x'] == 0]['gastos'].mean()

diferenca_medias = media_gastos_contratantes - media_gastos_nao_contratantes

print("Probabilidade de contratação do produto X para os clientes que não o contrataram:", probabilidade_contratacao[:, 1])
print("Diferença de médias de gastos entre os clientes que contrataram e os que não contrataram o produto X:", diferenca_medias)
