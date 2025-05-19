# Repositório de Curadoria da Rede SONDA

## Sobre a Rede SONDA
A SONDA (Sistema de Organização Nacional de Dados Ambientais) é uma rede de estações de coleta de dados ambientais que registra continuamente medições de variáveis meteorológicas, solarimétricas e anemométricas em diferentes regiões do Brasil. Esses dados são fundamentais para estudos climáticos, energéticos e ambientais.

## Objetivo
Este repositório contém scripts e notebooks para a curadoria, análise exploratória e validação dos dados da rede SONDA. O processo de curadoria visa identificar, documentar e tratar inconsistências nos dados, garantindo maior confiabilidade para aplicações científicas e técnicas.

## Estrutura de Dados
Os dados da rede SONDA estão organizados em três categorias principais:

1. **Dados Solarimétricos**: Medições de radiação solar (global, direta, difusa)
2. **Dados Meteorológicos**: Temperatura, umidade, pressão atmosférica, etc.
3. **Dados Anemométricos**: Velocidade e direção do vento em diferentes alturas

## Notebooks Disponíveis

### 1. Análise Exploratória de Dados Solarimétricos
Arquivo: 01_Analise_Exploratoria_Solarimetricos.ipynb
[![Abrir no Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/labren/sonda-curadoria/blob/main/01_Analise_Exploratoria_Solarimetricos.ipynb)
- Importação e visualização inicial dos dados solarimétricos
- Análise temporal da disponibilidade de dados por estação
- Visualização de séries temporais específicas (ex: estação BRB)
- Análise de dados em quarentena (potencialmente problemáticos)

### 2. Análise Exploratória de Dados Formatados
Arquivo: 02_Analise_Exploratoria_Formatados.ipynb
- Análise integrada dos três tipos de dados (meteorológicos, solarimétricos e anemométricos)
- Verificação da cobertura temporal dos dados por estação
- Identificação e quantificação de dados faltantes ou inválidos
- Análise estatística descritiva com visualizações temporais

## Metodologia de Curadoria
O processo de curadoria utiliza as seguintes técnicas:

1. **Identificação de valores inválidos**: Detecção de códigos especiais (3333.0, -5555.0) ou valores nulos
2. **Análise de completude**: Quantificação da porcentagem de dados válidos por variável e estação
3. **Verificação temporal**: Análise da continuidade e distribuição temporal dos dados
4. **Visualização de tendências**: Análise de séries temporais para identificar anomalias

## Como Utilizar

1. Clone este repositório:
   ```
   git clone https://github.com/labren/sonda-curadoria.git
   ```

2. Execute os notebooks Jupyter para realizar análises específicas:
   - Use o Google Colab (links disponíveis nos notebooks)
   - Ou execute localmente com um ambiente Python que inclua as dependências necessárias

3. Os dados necessários serão baixados automaticamente via gdown durante a execução dos notebooks

## Dependências
- Python 3.6+
- duckdb
- pandas
- matplotlib
- seaborn
- gdown (para download de arquivos)

## Contribuição
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests com melhorias nos scripts de curadoria ou novas análises.