"""
Script para gerar Gráfico #1: Throughput vs Tempo (APPEND - Sem Falhas)
Uso: Modifica as variáveis 'files_to_plot' para incluir os CSVs que queres comparar
COM GRID DETALHADO
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
    'blockchain-4nodes-faulty60000ms-append.csv': 'blockchain (4 nodes) node down at 60ms',
    'blockchain-4nodes-nofaulty2-append.csv': 'blockchain (4 nodes) no fault',
}

# Configurações do gráfico
TITLE = 'Throughput vs Time (APPEND - no faulty vs node down at 60s)'
Y_LABEL = 'Throughput (operations/s)'
X_LABEL = 'Time (s)'
OUTPUT_FILENAME = 'blockchain_throughput_faultyVSnofaulty.png'
FIGSIZE = (14, 8)
DPI = 300

# Nome da coluna de tempo (pode variar entre ficheiros)
TIME_COLUMN = 'Time(ms)'  # Usar 'timestamp_ms' se for Blockchain
THROUGHPUT_COLUMN = 'Count'

# ============================================================================
# FUNÇÃO PRINCIPAL - NÃO ALTERES ISTO
# ============================================================================

def load_and_plot():
    
    fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
    
    for filepath, label in files_to_plot.items():
        try:
           
            df = pd.read_csv(filepath)

            if TIME_COLUMN in df.columns:
                x = df[TIME_COLUMN].values / 1000  # Converter para segundos
            else:
                print(f"Aviso: Coluna '{TIME_COLUMN}' não encontrada em {filepath}")
                continue
            
           
            if THROUGHPUT_COLUMN in df.columns:
                y = df[THROUGHPUT_COLUMN].values
            else:
                print(f"Aviso: Coluna '{THROUGHPUT_COLUMN}' não encontrada em {filepath}")
                continue
            
            
            ax.plot(x, y, marker='o', markersize=4, linewidth=2, label=label, alpha=0.8)
            
            print(f"✓ Carregado: {filepath}")
            print(f"  - Throughput médio: {y.mean():.2f} ops/s")
            print(f"  - Throughput máximo: {y.max():.2f} ops/s")
            print(f"  - Throughput mínimo: {y.min():.2f} ops/s\n")
        
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
    ax.legend(fontsize=11, loc='best', framealpha=0.95)
    
    plt.tight_layout()
    
    # Guardar
    plt.savefig(OUTPUT_FILENAME, dpi=DPI, bbox_inches='tight')
    print(f"Gráfico guardado: {OUTPUT_FILENAME}")
    
    plt.show()


if __name__ == '__main__':
    load_and_plot()
