"""
Script para gerar Gráfico #2: Latência Média vs Tempo (APPEND - Com e Sem Falhas)
COM ESTILOS DE LINHA DIFERENTES POR PROTOCOLO
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
from matplotlib.ticker import MultipleLocator

# ============================================================================
# CONFIGURAÇÃO - MODIFICA AQUI PARA CADA GRÁFICO
# ============================================================================

# Lista de ficheiros a plotar (alterar conforme necessário)
files_to_plot = {
    'PBFT-5nodes(nofaulty)-append.csv': {
        'label': 'PBFT (5 nodes) - no faults',
        'linestyle': '-',      # Sólido
        'color': '#1f77b4',    # Azul
        'linewidth': 2.5
    },
    'PBFT-5nodes(faulty60000ms)-append.csv': {
        'label': 'PBFT (5 nodes) - faulty',
        'linestyle': '--',     # Tracejado
        'color': '#1f77b4',    # Azul (mesmo protocolo)
        'linewidth': 2.5
    },
    'Unordered-4nodes(nofaulty)-APPEND.csv': {
        'label': 'Unordered (4 nodes) - no faults',
        'linestyle': '-',      # Sólido
        'color': '#ff7f0e',    # Laranja
        'linewidth': 2.5
    },
    'Unordered-4nodes(faulty60000ms)-append.csv': {
        'label': 'Unordered (4 nodes) - faulty',
        'linestyle': '--',     # Tracejado
        'color': '#ff7f0e',    # Laranja (mesmo protocolo)
        'linewidth': 2.5
    },
    'blockchain-4nodes-nofaulty2-append.csv': {
        'label': 'Blockchain (4 nodes) - no faults',
        'linestyle': '-',      # Sólido
        'color': '#2ca02c',    # Verde
        'linewidth': 2.5
    },
    'blockchain-4nodes-faulty60000ms-append.csv': {
        'label': 'Blockchain (4 nodes) - faulty',
        'linestyle': '--',     # Tracejado
        'color': '#2ca02c',    # Verde (mesmo protocolo)
        'linewidth': 2.5
    },
}

# Configurações do gráfico
TITLE = 'Latency vs Time (APPEND - With and Without Faults (node down at 60ms in faultys))'
Y_LABEL = 'Latency (ms)'
X_LABEL = 'Time (s)'
OUTPUT_FILENAME = 'ALL_latency_append_faulty_comparison.png'
FIGSIZE = (16, 9)
DPI = 300

# Nome da coluna de tempo (pode variar entre ficheiros)
TIME_COLUMN = 'Time(ms)'  # Usar 'timestamp_ms' se for Blockchain
LATENCY_COLUMN = 'Avg(ms)'  # Latência média

# ============================================================================
# FUNÇÃO PRINCIPAL - NÃO ALTERES ISTO
# ============================================================================

def detect_columns(df):
    """Detecta automaticamente os nomes de colunas (Blockchain vs PBFT/Unordered)."""
    
    # Verificar coluna de tempo
    if 'timestamp_ms' in df.columns:
        time_col = 'timestamp_ms'
    elif 'Time(ms)' in df.columns:
        time_col = 'Time(ms)'
    else:
        raise ValueError("Coluna de tempo não encontrada!")
    
    # Verificar coluna de latência
    if 'avg' in df.columns:
        latency_col = 'avg'
    elif 'Avg(ms)' in df.columns:
        latency_col = 'Avg(ms)'
    else:
        raise ValueError("Coluna de latência não encontrada!")
    
    return time_col, latency_col

def load_and_plot():
    
    fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
    
    for filepath, style in files_to_plot.items():
        try:
           
            df = pd.read_csv(filepath)
            
            # Detectar colunas automaticamente
            time_col, latency_col = detect_columns(df)

            if time_col in df.columns:
                x = df[time_col].values / 1000  # Converter para segundos
            else:
                print(f"Aviso: Coluna '{time_col}' não encontrada em {filepath}")
                continue
            
           
            if latency_col in df.columns:
                y = df[latency_col].values
            else:
                print(f"Aviso: Coluna '{latency_col}' não encontrada em {filepath}")
                continue
            
            # Plotar com estilos personalizados
            ax.plot(
                x, y,
                marker='o',
                markersize=3,
                linewidth=style['linewidth'],
                label=style['label'],
                alpha=0.85,
                linestyle=style['linestyle'],
                color=style['color']
            )
            
            print(f"✓ Carregado: {filepath}")
            print(f"  - Latência média: {y.mean():.2f} ms")
            print(f"  - Latência máxima: {y.max():.2f} ms")
            print(f"  - Latência mínima: {y.min():.2f} ms\n")
        
        except FileNotFoundError:
            print(f"✗ Ficheiro não encontrado: {filepath}")
        except Exception as e:
            print(f"✗ Erro ao processar {filepath}: {e}")
    
    # ========================================================================
    # ADICIONAR GRID DETALHADO (LINHAS MAIS GRANULARES)
    # ========================================================================
    
    # Eixo Y - Ticks maiores a cada 50, ticks menores a cada 10
    ax.yaxis.set_major_locator(MultipleLocator(50))
    ax.yaxis.set_minor_locator(MultipleLocator(10))
    
    # Eixo X - Ticks maiores a cada 10 segundos, ticks menores a cada 2 segundos
    ax.xaxis.set_major_locator(MultipleLocator(10))
    ax.xaxis.set_minor_locator(MultipleLocator(2))
    
    # Grid com linhas sólidas (maiores) e tracejadas (menores)
    ax.grid(True, which='major', alpha=0.5, linestyle='-', linewidth=0.7)
    ax.grid(True, which='minor', alpha=0.2, linestyle='--', linewidth=0.3)
    
    # Configurar aspecto do gráfico
    ax.set_title(TITLE, fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel(X_LABEL, fontsize=12, fontweight='bold')
    ax.set_ylabel(Y_LABEL, fontsize=12, fontweight='bold')
    ax.legend(fontsize=10, loc='best', framealpha=0.95, ncol=2)  # 2 colunas na legenda
    
    plt.tight_layout()
    
    # Guardar
    plt.savefig(OUTPUT_FILENAME, dpi=DPI, bbox_inches='tight')
    print(f"Gráfico guardado: {OUTPUT_FILENAME}")
    
    plt.show()



if __name__ == '__main__':
    load_and_plot()
