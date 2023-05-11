import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import precision_score
import optuna


# Carrega os dados
data = pd.read_csv('caminho/para/arquivo.csv')

# Separa os dados em X (variáveis explicativas) e y (variável target)
X = data.drop('target', axis=1)
y = data['target']

# Divide os dados em treino e teste, com estratificação
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Define a função objetivo para otimização com o Optuna
def objective(trial):

    # Define os hiperparâmetros a serem otimizados
    max_depth = trial.suggest_int('max_depth', 2, 10)
    min_samples_split = trial.suggest_int('min_samples_split', 2, 20)
    min_samples_leaf = trial.suggest_int('min_samples_leaf', 1, 10)
    
    # Define o modelo de árvore de decisão com os hiperparâmetros
    model = DecisionTreeClassifier(
        max_depth=max_depth,
        min_samples_split=min_samples_split,
        min_samples_leaf=min_samples_leaf,
        random_state=42
    )
    
    # Realiza a validação cruzada com amostra estratificada para calcular a precisão média do modelo
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = []
    for train_idx, val_idx in cv.split(X_train, y_train):
        X_train_fold, y_train_fold = X_train.iloc[train_idx], y_train.iloc[train_idx]
        X_val_fold, y_val_fold = X_train.iloc[val_idx], y_train.iloc[val_idx]
        
        model.fit(X_train_fold, y_train_fold)
        y_pred_fold = model.predict(X_val_fold)
        score_fold = precision_score(y_val_fold, y_pred_fold)
        scores.append(score_fold)
    score = sum(scores) / len(scores)
    
    return score

# Define o estudo do Optuna
study = optuna.create_study(direction='maximize')
study.optimize(objective, n_trials=100)

# Imprime os resultados
print('Valor ótimo: ', study.best_value)
print('Melhores hiperparâmetros: ', study.best_params)
