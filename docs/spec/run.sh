#!/usr/bin/env bash
# Model-check one configuration with TLC.
#
#   usage: ./run.sh <cfg-name> [extra TLC args]
#   e.g.   ./run.sh Fix_token
#          TLA2TOOLS=/opt/tla/tla2tools.jar WORKERS=8 HEAP=8g ./run.sh S6_design
#
# The spec is chosen from the cfg name prefix (see README.md). Output goes to
# stdout. TLC writes its state files under ./states/ (safe to delete).
set -euo pipefail
cd "$(dirname "$0")"

cfg=${1:?usage: ./run.sh <cfg-name> [extra TLC args]}
shift

case $cfg in
  Bug_*)                            spec=ChunkLifecycle ;;
  Fix_*)                            spec=ChunkLifecycleFixed ;;
  W4_*|RED_*)                       spec=WorkspaceTakeover ;;
  S3_wal_*|S3v*|S3r_*|S3m_*|S3f_*)  spec=WALRecovery ;;
  S3_sw*)                           spec=SmartWALRecovery ;;
  S3_rep_*)                         spec=WALReplica ;;
  S4b_*)                            spec=CutPipeline ;;
  S4_*)                             spec=CutCapture ;;
  S5_*)                             spec=ControlPlane ;;
  S6_*)                             spec=ReplicationFailover ;;
  S9b_*)                            spec=RetentionGCMulti ;;
  S9_*)                             spec=RetentionGCTiming ;;
  *) echo "run.sh: no spec for config '$cfg'" >&2; exit 2 ;;
esac

jar=${TLA2TOOLS:-tla2tools.jar}
[ -f "$jar" ] || { echo "run.sh: $jar not found; set TLA2TOOLS (see README.md)" >&2; exit 2; }

# -deadlock: deadlock checking off. Several specs have a process that loops
# forever (the GC), and bounded models legitimately stop at their bounds.
exec java -XX:+UseParallelGC "-Xmx${HEAP:-4g}" -cp "$jar" tlc2.TLC \
  -config "cfg/$cfg.cfg" -deadlock -workers "${WORKERS:-auto}" \
  -metadir "states/$cfg" "$@" "$spec.tla"
