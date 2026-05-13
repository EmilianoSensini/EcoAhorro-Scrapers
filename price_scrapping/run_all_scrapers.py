import subprocess
import os
import sys
import time

def run_scraper(scraper_dir, scraper_script):
    """
    Inicia un scraper en un subproceso, asegurándose de que el directorio de trabajo
    sea el del scraper para que los archivos de salida se guarden correctamente.
    """
    abs_dir = os.path.abspath(scraper_dir)
    script_path = os.path.join(abs_dir, scraper_script)
    
    if not os.path.exists(script_path):
        print(f"Error: No se encontró el script {script_path}")
        return None
    
    print(f"Iniciando scraper: {scraper_script} en {scraper_dir}...")
    
    # Usamos sys.executable para asegurarnos de usar el mismo entorno de Python
    return subprocess.Popen([sys.executable, scraper_script], cwd=abs_dir)

def main():
    # Definición de los scrapers y sus directorios
    scrapers = [
        ("changoMasScrapping", "masOnline_scraper.py"),
        ("coopeScrapping", "coope_scraper.py"),
        ("laBanderitaScrapping", "laBanderita_scraper.py"),
        ("veaScrapping", "vea_scraper.py")
    ]
    
    # Cambiamos al directorio donde está este script para que las rutas relativas funcionen
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    processes = []
    
    # Iniciar todos los scrapers
    for dir_name, script_name in scrapers:
        p = run_scraper(dir_name, script_name)
        if p:
            processes.append((script_name, p))
    
    if not processes:
        print("No se pudo iniciar ningún scraper.")
        return

    print(f"\nSe han iniciado {len(processes)} scrapers en paralelo.\n")
    
    # Monitorear procesos
    try:
        while processes:
            for i, (name, p) in enumerate(processes):
                poll = p.poll()
                if poll is not None:
                    # El proceso ha terminado
                    if poll == 0:
                        print(f"  {name} ha finalizado exitosamente.")
                    else:
                        print(f" {name} ha finalizado con error (código {poll}).")
                    processes.pop(i)
                    break # Salir del loop interno para evitar problemas con pop()
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nInterrupción detectada. Deteniendo todos los scrapers...")
        for name, p in processes:
            p.terminate()
        print("Procesos terminados.")

    print("\n Todos los scrapers han finalizado.")

if __name__ == "__main__":
    main()
