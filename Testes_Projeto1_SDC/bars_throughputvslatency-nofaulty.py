"""
Gráfico de Barras Duplo: Throughput Médio vs Latência Média (APPEND - no faults)
COM DOIS EIXOS Y (esquerda e direita)
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import numpy as np

# ============================================================================
# CONFIGURAÇÃO - MODIFICA AQUI PARA CADA GRÁFICO
# ============================================================================

# Ficheiros a processar (sem falhas)
files_nofaulty = {
    'PBFT-5nodes(faulty60000ms)-append.csv': 'PBFT (5 nodes (node down at 60s))',
    'Unordered-4nodes(nofaulty)-APPEND.csv': 'Unordered (4 nodes)',
    'blockchain-4nodes-nofaulty2-append.csv': 'Blockchain (4 nodes)',
}

# Configurações do gráfico
TITLE = 'Throughput/Latency'
OUTPUT_FILENAME = 'comparison_throughput_latency.png'
FIGSIZE = (14, 8)
DPI = 300

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def detect_columns(df):
    """Detecta automaticamente os nomes de colunas."""
    
    # Coluna de tempo
    if 'timestamp_ms' in df.columns:
        time_col = 'timestamp_ms'
    elif 'Time(ms)' in df.columns:
        time_col = 'Time(ms)'
    else:
        raise ValueError("Coluna de tempo não encontrada!")
    
    # Coluna de throughput
    if 'count' in df.columns:
        throughput_col = 'count'
    elif 'Count' in df.columns:
        throughput_col = 'Count'
    else:
        raise ValueError("Coluna de throughput não encontrada!")
    
    # Coluna de latência
    if 'avg' in df.columns:
        latency_col = 'avg'
    elif 'Avg(ms)' in df.columns:
        latency_col = 'Avg(ms)'
    else:
        raise ValueError("Coluna de latência não encontrada!")
    
    return time_col, throughput_col, latency_col

def load_and_plot():
    
    # Dicionários para armazenar os dados
    protocols = []
    throughputs = []
    latencies = []
    
    for filepath, label in files_nofaulty.items():
        try:
            df = pd.read_csv(filepath)
            
            # Detectar colunas
            time_col, throughput_col, latency_col = detect_columns(df)
            
            # Calcular médias
            avg_throughput = df[throughput_col].mean()
            avg_latency = df[latency_col].mean()
            
            protocols.append(label)
            throughputs.append(avg_throughput)
            latencies.append(avg_latency)
            
            print(f"✓ Carregado: {filepath}")
            print(f"  - Throughput médio: {avg_throughput:.2f} ops/s")
            print(f"  - Latência média: {avg_latency:.2f} ms\n")
        
        except FileNotFoundError:
            print(f"✗ Ficheiro não encontrado: {filepath}")
        except Exception as e:
            print(f"✗ Erro ao processar {filepath}: {e}")
    
    # ========================================================================
    # CRIAR GRÁFICO DE BARRAS DUPLO COM DOIS EIXOS Y
    # ========================================================================
    
    fig, ax1 = plt.subplots(figsize=FIGSIZE, dpi=DPI)
    
    # Posições das barras
    x = np.arange(len(protocols))
    width = 0.35
    
    # Cores
    color_throughput = '#1f77b4'  # Azul
    color_latency = '#ff7f0e'      # Laranja
    
    # Plotar barras de Throughput no eixo esquerdo
    bars1 = ax1.bar(x - width/2, throughputs, width, label='Throughput (ops/s)',
                    color=color_throughput, alpha=0.8)
    ax1.set_ylabel('Throughput (operations/s)', fontsize=12, fontweight='bold', color=color_throughput)
    ax1.tick_params(axis='y', labelcolor=color_throughput)
    ax1.yaxis.set_major_locator(MultipleLocator(50))
    ax1.yaxis.set_minor_locator(MultipleLocator(10))
    
    # Criar eixo direito para Latência
    ax2 = ax1.twinx()
    bars2 = ax2.bar(x + width/2, latencies, width, label='Latency (ms)',
                    color=color_latency, alpha=0.8)
    ax2.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold', color=color_latency)
    ax2.tick_params(axis='y', labelcolor=color_latency)
    ax2.yaxis.set_major_locator(MultipleLocator(50))
    ax2.yaxis.set_minor_locator(MultipleLocator(10))
    
    # Configurar eixo X
    ax1.set_xlabel('Protocol', fontsize=12, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(protocols, fontsize=11)
    
    # Título
    fig.suptitle(TITLE, fontsize=16, fontweight='bold', y=0.98)
    
    # Grid no eixo esquerdo
    ax1.grid(True, which='major', alpha=0.3, linestyle='-', linewidth=0.7, axis='y')
    ax1.grid(True, which='minor', alpha=0.1, linestyle='--', linewidth=0.3, axis='y')
    
    # Adicionar valores nas barras
    for i, (bar, value) in enumerate(zip(bars1, throughputs)):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{value:.0f}',
                ha='center', va='bottom', fontsize=9, fontweight='bold', color=color_throughput)
    
    for i, (bar, value) in enumerate(zip(bars2, latencies)):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{value:.0f}',
                ha='center', va='bottom', fontsize=9, fontweight='bold', color=color_latency)
    
    # Legenda
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=11, framealpha=0.95)
    
    plt.tight_layout()
    
    # Guardar
    plt.savefig(OUTPUT_FILENAME, dpi=DPI, bbox_inches='tight')
    print(f"✓ Gráfico guardado: {OUTPUT_FILENAME}")
    
    plt.show()

if __name__ == '__main__':
    load_and_plot()
