#!/usr/bin/env bash
# Corre EN LA EC2. Opera los DAGs del proyecto dentro del contenedor del scheduler, sin abrir la interfaz web.
#   ./orquestacion/dag.sh activar <dag>                   quita la pausa (con catchup=False corre el ultimo intervalo)
#   ./orquestacion/dag.sh pausar <dag>
#   ./orquestacion/dag.sh disparar <dag> <run_id> [conf]  conf en JSON, por ejemplo '{"fallo_controlado":"carga_a_mitad"}'
#   ./orquestacion/dag.sh ultima <dag>                    run_id de la ejecucion mas reciente
#   ./orquestacion/dag.sh esperar <dag> <run_id>          espera hasta success (0) o failed (1), maximo 60 minutos
#   ./orquestacion/dag.sh tareas <dag> <run_id>           estado, intentos y duracion de cada tarea
#   ./orquestacion/dag.sh ejecuciones <dag>
#   ./orquestacion/dag.sh log <dag> <run_id> <tarea> <patron>   lineas del log de cada intento que cumplen el patron
set -euo pipefail
cd "$(dirname "$0")"
af() { docker compose exec -T airflow-scheduler "$@"; }
py() { af python - "$@" 2>/dev/null; }  # el script llega por stdin; los avisos de Airflow van a stderr

estado() {
  py "$1" "$2" <<'PY'
import sys
from airflow.models import DagRun
runs = DagRun.find(dag_id=sys.argv[1], run_id=sys.argv[2])
print(runs[0].state if runs else "no_existe")
PY
}

case "${1:-}" in
  activar) af airflow dags reserialize >/dev/null 2>&1 || true; af airflow dags unpause "$2" ;;
  pausar) af airflow dags pause "$2" ;;
  disparar)
    conf='{}'
    if [ $# -ge 4 ]; then conf=$4; fi
    af airflow dags trigger "$2" --run-id "$3" --conf "$conf" 2>/dev/null | tail -4 ;;
  ultima)
    py "$2" <<'PY'
import sys
from airflow.models import DagRun
runs = sorted(DagRun.find(dag_id=sys.argv[1]), key=lambda r: r.execution_date, reverse=True)
print(runs[0].run_id if runs else "")
PY
    ;;
  esperar)
    for i in $(seq 1 120); do
      s=$(estado "$2" "$3")
      case "$s" in
        success) echo "$(date +%H:%M:%S) $3: success"; exit 0 ;;
        failed) echo "$(date +%H:%M:%S) $3: failed"; exit 1 ;;
      esac
      if [ $((i % 4)) = 1 ]; then echo "$(date +%H:%M:%S) $3: $s"; fi
      sleep 30
    done
    echo "$3 no termino en 60 minutos"; exit 1 ;;
  tareas)
    py "$2" "$3" <<'PY'
import sys
from airflow.models import DagRun
run = DagRun.find(dag_id=sys.argv[1], run_id=sys.argv[2])[0]
print(f"{run.run_id} | {run.run_type} | {run.state} | conf {run.conf or {}}")
print(f"{'tarea':24} {'estado':16} {'intentos':>8} {'segundos':>9}")
order = lambda ti: (ti.start_date is None, ti.start_date or 0)
for ti in sorted(run.get_task_instances(), key=order):
    dur = f"{ti.duration:.0f}" if ti.duration else ""
    print(f"{ti.task_id:24} {str(ti.state):16} {ti.try_number:>8} {dur:>9}")
PY
    ;;
  ejecuciones)
    py "$2" <<'PY'
import sys
from airflow.models import DagRun
print(f"{'run_id':52} {'tipo':10} {'estado':8} {'inicio (UTC)':20} conf")
for r in sorted(DagRun.find(dag_id=sys.argv[1]), key=lambda r: r.execution_date):
    inicio = r.start_date.strftime('%Y-%m-%d %H:%M:%S') if r.start_date else ''
    print(f"{r.run_id:52} {r.run_type:10} {r.state:8} {inicio:20} {r.conf or ''}")
PY
    ;;
  log)
    af bash -c 'for f in /opt/airflow/logs/dag_id="$1"/run_id="$2"/task_id="$3"/attempt=*.log; do
                  [ -f "$f" ] || continue; echo "== ${f##*/}"; grep -h -i -E "$4" "$f" | cut -c1-240; done' _ "$2" "$3" "$4" "$5" ;;
  *) sed -n '2,11p' "$0"; exit 2 ;;
esac
