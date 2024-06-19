
## QueryMiner MySQL

Este script Python é uma ferramenta para organizar os dados do arquivo GeneralLog do MySQL.

### Funcionalidades
- Principais:
	- **Volumetria:** Calcula a frequência de cada consulta e gera um arquivo com a lista ordenada por frequência.
	- **Organização por thread:** Agrupa as consultas por ID de thread.
	- **Opções de execução:** Permite escolher entre processamento em disco ou memória.
	- **Organização em Lotes:** Agrupa as threads em lotes, facilitando a reexecução desses arquivos no ambiente.
- Segundarias:
	- **Verbose:** Fornece mensagens adicionais para debug.
	- **Limpeza:** Permite excluir os arquivos e pastas criados.

### Pré-requisitos

* **Python 3**
* **SO Linux**

### Instalação

```bash
pip install -r requirements.txt
```

### Uso

```bash
python queryMiner.py -log <caminho_do_arquivo_de_log> [opções]
```

**Opções:**

* **-log [DIR]:**  Caminho para o arquivo de log do MySQL (default: log.txt).
* **-disk:** Executa o processamento em disco ao invés de memória (mais lento).
* **-merge:** Habilita a junção das threads ao final do processo.
* **-mode {0,1}:** Modo de junção:
    * 0 - THREADS (default): Define o numero de threads por lote.
    * 1 - LOTES: Define o numero de lotes.
* **-mergenum NUM:** Número de junções/lotes (default: 5).
* **-vol:** Habilita a volumetria.
* **-verbose:** Modo detalhado com mensagens adicionais.
* **-clear:** Exclui os arquivos e pastas criados.

### Exemplos

* **Organizar o log.txt com 10 threads por lote:**
   ```bash
   python queryMiner.py -log log.txt -merge -mergenum 10
   ```

* **Organizar o my_log.txt com volumetria:**
   ```bash
   python queryMiner.py -log my_log.txt -vol
   ```

### Notas

* O script utiliza expressões regulares para identificar as consultas SQL no arquivo de log.
* O script pode lidar com consultas que se estendem por várias linhas.
* A performance do script pode variar dependendo do tamanho do arquivo de log e das opções de execução.