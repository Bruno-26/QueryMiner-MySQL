import argparse
import os
import re
import shutil
import subprocess
import sys
import time

from memory_profiler import profile

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"
BLUE = "\033[94m"


def print_ok(text):
    print(f"{GREEN}OK: {text}{RESET}\n")


def print_info(text):
    print(f"{BLUE}INFO: {text}{RESET}\n", flush=True)


def print_error(text):
    print(f"{RED}ERROR: {text}{RESET}\n")


def print_separator(width):
    print("+" + "-" * width + "+")


def print_table(title, data):
    max_key_length = max(len(key) for key in data.keys())
    max_value_length = max(len(str(value)) for value in data.values())

    total_width = max(max_key_length, max_value_length)

    print_separator(total_width * 2 + 4)
    print(f"|  {BLUE}{title.center(total_width*2)}{RESET}  |")
    print_separator(total_width * 2 + 4)
    for key, value in data.items():
        print(f"| {key.ljust(total_width)} | {str(value).ljust(total_width)}|")
    print_separator(total_width * 2 + 4)


def verificar_arquivo(file):
    if not os.path.exists(file):
        print_error(f"O arquivo '{file}' não foi encontrado.")
        sys.exit(1)
    else:
        print_ok(f"O arquivo '{file}' foi encontrado.")


def create_dir(dir):
    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
            print_ok(f"Diretório '{dir}' criado.")
        else:
            print_ok(f"Diretório '{dir}' existente.")
    except Exception as e:
        print_error(f"Erro ao criar o diretório '{dir}': {e}")

def remove(dir):
    try:
        if os.path.isfile(dir):
            os.remove(dir)
            print_ok(f"Arquivo {dir} excluído.")
        elif os.path.isdir(dir):
            shutil.rmtree(dir)
            print_ok(f"Pasta {dir} e seu conteúdo foram excluídos.")
    except Exception as e:
        print_error(f"Erro ao remover o arquivo {dir}: {e}")

def cleaner(*args):
    for name in args:
        if name.endswith('*'):
            name=name[:-1]
            for filename in os.listdir():
                if filename.startswith(name):
                    remove(filename)
        else:
            for filename in os.listdir():
                if filename == name:
                    remove(filename)



def batch_files(dir, num, mode, verbose):
    arquivos = []

    for nome_arquivo in os.listdir(dir):
        if os.path.isfile(os.path.join(dir, nome_arquivo)) and nome_arquivo.endswith(
            ".sql"
        ):
            if nome_arquivo.startswith("thread"):
                arquivos.append(os.path.join(dir, nome_arquivo))

    if mode == 0:
        print_info(f"Modo: {num} threads por lote")
        # Divide os arquivos em lotes
        lotes = [arquivos[i : i + num] for i in range(0, len(arquivos), num)]

        # Exibe os lotes
        with open("log-batches.txt", "w") as batchlog:
            for i, lote in enumerate(lotes):
                batchlog.write(f"Lote {i+1}: {lote}\n")
                if verbose:
                    print(f"{GREEN}Lote {i+1}:{RESET} {lote}\n")

        for i in range(0, len(arquivos), num):
            lote = arquivos[i : i + num]
            nome_saida = f"batches/lote_{i // num + 1}.sql"
            with open(nome_saida, "w") as f_saida:
                for nome_arquivo in lote:
                    with open(nome_arquivo, "r") as f:
                        f_saida.write(f.read())
    else:
        arquivos_por_lote = len(arquivos) // num

        print_info(f"Modo: {num} lotes => {arquivos_por_lote} threads por lote")
        for i in range(num):
            if i == num - 1:
                lote = arquivos[i * arquivos_por_lote :]
            else:
                lote = arquivos[i * arquivos_por_lote : (i + 1) * arquivos_por_lote]
            nome_lote = f"batches/lote_{i+1}.sql"
            with open(nome_lote, "w") as f:
                for arquivo in lote:
                    with open(os.path.join("", arquivo), "r") as arquivo_origem:
                        for linha in arquivo_origem:
                            f.write(linha)
                    if verbose:
                        print(f"{arquivo} adicionado ao {nome_lote}")

        with open("log-batches.txt", "w") as batchlog:
            for i in range(num):
                if i == num - 1:
                    lote = arquivos[i * arquivos_por_lote :]
                else:
                    lote = arquivos[i * arquivos_por_lote : (i + 1) * arquivos_por_lote]
                batchlog.write(f"Lote {i+1}: [")
                for arquivo in lote:
                    batchlog.write(f"'{arquivo}', ")
                if verbose:
                    print(f"Lote {i+1} registrado no log-batches.txt")
                batchlog.write(f"]\n")


def process_mysql_log(input_file, disk, verbose):
    print_info("Iniciando a analise")
    # Regex
    pattern = r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(\d+)\s+(Query|Execute|Prepare)\s+(.*)$"
    pattern2 = r"^(/rdsdbbin/mysql/|Tcp port:|Time|\d{4}-\d{2}-\d{2}T)"

    # Dicionário
    threads = {}

    num_linhas = 0

    # Abrir o arquivo em modo de leitura
    with open(input_file, "r") as arquivo:
        for _ in arquivo:
            num_linhas += 1
    num_linhas = "{:,}".format(num_linhas).replace(",", ".")

    with open(input_file, "r") as f:
        # Lista temporária para as linhas da consulta
        current_query_lines = []

        current_thread_id = None
        cont = 0
        for line in f:
            cont += 1
            if (cont % 100000) == 0:
                print(
                    f"{GREEN}Linhas processadas: {'{:,}'.format(cont).replace(',', '.')} of {num_linhas}{RESET}",
                    end="\r",
                )

            match = re.match(pattern, line)
            if match:
                timestamp = match.group(1)
                thread_id = match.group(2)
                query = match.group(4)

                if query.startswith("CREATE") or query.startswith("DROP"):
                    continue

                # Verificar se é uma nova consulta
                if current_thread_id != thread_id:
                    # Se sim, salvar a consulta anterior (se houver)
                    if current_query_lines:
                        if not disk:
                            threads[current_thread_id].append(
                                " ".join(current_query_lines) + ";"
                            )
                        else:
                            with open(
                                f"threads/thread_{current_thread_id}.sql", "a"
                            ) as arquivo:
                                arquivo.write(" ".join(current_query_lines) + ";\n")

                    # Iniciar uma nova lista de linhas
                    current_query_lines = [query]
                    current_thread_id = thread_id

                    # Criar uma nova lista/arquivo para o thread ID
                    if not disk:
                        if thread_id not in threads:
                            threads[thread_id] = []
                    else:
                        if not os.path.exists(f"threads/thread_{thread_id}.sql"):
                            with open(f"threads/thread_{thread_id}.sql", "w"):
                                pass

                else:
                    if current_query_lines:
                        if not disk:
                            threads[current_thread_id].append(
                                " ".join(current_query_lines) + ";"
                            )
                        else:
                            with open(
                                f"threads/thread_{current_thread_id}.sql", "a"
                            ) as arquivo:
                                arquivo.write(" ".join(current_query_lines) + ";\n")
                    # Se é uma continuação da consulta anterior, adicionar à lista temporária
                    current_query_lines = [query]

            else:
                if re.match(pattern2, line):
                    if verbose:
                        print("Aviso: linha de log ignorada:", line.strip())
                else:
                    if current_query_lines:
                        current_query_lines[-1] += " " + line.strip()
                    else:
                        if verbose:
                            print("Aviso: linha de log ignorada:", line.strip())

        # Adicionar a última consulta
        if current_query_lines:
            if not disk:
                threads[current_thread_id].append(" ".join(current_query_lines) + ";")
            else:
                with open(f"threads/thread_{current_thread_id}.sql", "a") as arquivo:
                    arquivo.write(" ".join(current_query_lines) + ";\n")

    print(
        f"{GREEN}Linhas processadas: {'{:,}'.format(cont).replace(',', '.')} of {num_linhas}{RESET}",
        end="\r",
    )
    print(" " * 60, end="\r", flush=True)
    return threads


def create_threads_files(threads):
    print_info("Iniciando a criação dos arquivos")
    for thread_id, queries in threads.items():
        output_file = f"threads/thread_{thread_id}.sql"
        with open(output_file, "w") as f:
            # Escrever cada consulta em uma nova linha no arquivo
            for query in queries:
                f.write(query + "\n")


def ajustar_parenteses(querie):
    new_querie = ""
    contador_parenteses = 0

    for caractere in querie:
        if caractere == "(":
            contador_parenteses += 1
            if contador_parenteses == 1:
                new_querie += "("
        elif caractere == ")":
            contador_parenteses -= 1
            if contador_parenteses == 0:  # Apenas os parênteses externos
                new_querie += "?)"
        else:
            if contador_parenteses == 0:  # Apenas substituir fora dos parênteses
                new_querie += caractere
            else:
                new_querie += ""  # Ignorar dentro dos parênteses

    return new_querie


def volumetria(threads, disk):
    print_info("Iniciando a volumetria")
    volumetria = {}
    dir = "threads/"

    if disk:
        threads = []
        for nome_arquivo in os.listdir(dir):
            if os.path.isfile(
                os.path.join(dir, nome_arquivo)
            ) and nome_arquivo.endswith(".sql"):
                if nome_arquivo.startswith("thread"):
                    threads.append(os.path.join(dir, nome_arquivo))
        # tam = len(threads)
    # else:
    tam = "{:,}".format(len(threads)).replace(",", ".")

    cont = 0

    if disk:
        for i in range(0, len(threads)):
            cont += 1
            print(
                f"{GREEN}Threads processadas: {'{:,}'.format(cont).replace(',', '.')} of {tam}{RESET}",
                end="\r",
            )
            with open(threads[i], "r") as arquivo:
                for linha in arquivo:
                    querie = ajustar_parenteses(linha.strip())
                    querie = re.sub(r"'[^']*'", r"'?'", querie)
                    querie = re.sub(r"[-+]?[0-9]*\.?[0-9]+", "?", querie)

                    # querie = re.sub(r"\(((?:[^()]|(?R))*)\)", "(?)", querie)

                    if querie not in volumetria:
                        # volumetria[querie] = []
                        volumetria[querie] = 1
                    else:
                        volumetria[querie] += 1

                    # output_file = f"queries.sql"
                    # with open(output_file, "w") as f:
                    #     for querie, i in volumetria.items():
                    #         f.write(f"{i} - {querie} \n")

                    # if not os.path.exists(f"queries/{querie}"):
                    #     with open(f"queries/{querie}", 'w'):
                    #         pass
                    # with open(f"queries/{querie}", 'a') as f:
                    #     f.write(querie + "\n")

    else:
        for threadid, queries in threads.items():
            cont += 1
            print(
                f"{GREEN}Threads processadas: {'{:,}'.format(cont).replace(',', '.')} of {tam}{RESET}",
                end="\r",
            )
            for linha in queries:
                querie = ajustar_parenteses(linha.strip())
                querie = re.sub(r"'[^']*'", r"'?'", querie)
                querie = re.sub(r"[-+]?[0-9]*\.?[0-9]+", "?", querie)

                if querie not in volumetria:
                    volumetria[querie] = 1
                else:
                    volumetria[querie] = volumetria[querie] + 1

    output_file = f"queries.sql"
    with open(output_file, "w") as f:
        for querie, i in volumetria.items():
            f.write(f"{i} - {querie} \n")

    subprocess.check_call(f"sort -n -r -k1 {output_file} > volumetria.sql", shell=True)
    os.remove(output_file)

    print(" " * 60, end="\r", flush=True)


@profile
def main(logfile, disk, merge, mergenum, mode, vol, verbose, clear):
    start_time = time.time()

    if clear:
        cleaner(
            "log-batches.txt",
            "volumetria.sql",
            "threads",
            "batches",
            "mprofile*",
            "output.png",
        )
        sys.exit(1)

    parameters = {
        "Arquivo de Log": logfile,
        "Execução no disco": disk,
        "Criar lotes": merge,
        "Número de junções": "--" if not merge else mergenum,
        "Modo": "--" if not merge else mode,
        "Volumetria": vol,
        "Verbose": verbose,
    }
    print_table("Parametros", parameters)

    verificar_arquivo(logfile)
    create_dir("threads")

    threads = {}

    if merge:
        create_dir("batches")

    if not disk:
        threads = process_mysql_log(logfile, disk, verbose)
        create_threads_files(threads)
    else:
        process_mysql_log(logfile, disk, verbose)

    if vol:
        volumetria(threads, disk)

    if merge:
        print_info("Iniciando a junção dos arquivos")
        batch_files("threads", mergenum, mode, verbose)

    finaltime = time.time() - start_time

    hours = finaltime // 3600
    minutes = (finaltime % 3600) // 60
    seconds = finaltime % 60

    print_info(
        f"Operação concluida!\n\nTempo de execução: {hours:02.0f}:{minutes:02.0f}:{seconds:02.0f}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Este programa tem como objetivo organizar os dados do arquivo GeneralLog do MySQL, dividindo-os por threads/lotes e preparando-os para serem reexecutados no servidor."
    )
    parser.add_argument(
        "-log",
        "--logfile",
        default="log.txt",
        metavar="LOGFILE",
        help="Nome do arquivo de log (default: log.txt)",
    )
    parser.add_argument(
        "-disk",
        action="store_true",
        help="Altera a execução em disco em vez de memória (mais lento).",
    )
    parser.add_argument(
        "-merge",
        action="store_true",
        help="Habilitar junção das threads ao final do processo.",
    )
    parser.add_argument(
        "-mode",
        default=0,
        type=int,
        choices=[0, 1],
        help="Modo de junção [0 - THREADS] [1 - LOTES] (default: 0 - THREADS)",
    )
    parser.add_argument(
        "-mergenum",
        type=int,
        default=5,
        metavar="NUM",
        help="Número de junções/lotes (default: 5)",
    )
    parser.add_argument(
        "-vol",
        action="store_true",
        help="Habilita volumetria.",
    )
    parser.add_argument("-verbose", action="store_true", help="Modo detalhado")
    parser.add_argument(
        "-clear", action="store_true", help="Deleta os arquivos e pastas criados"
    )
    args = parser.parse_args()

    main(
        args.logfile,
        args.disk,
        args.merge,
        args.mergenum,
        args.mode,
        args.vol,
        args.verbose,
        args.clear,
    )
