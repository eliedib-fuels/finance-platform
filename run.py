"""
Finance Platform — Command Line Entry Point

Usage:
  python run.py --init
  python run.py --hierarchy
  python run.py --rates
  python run.py --ad50 data/AD50_Jan.xlsx
  python run.py --ad50 data/AD50_Jan.xlsx --dry-run
  python run.py --gl data/jan_feb/577 GL Details_Jan26.xlsx
  python run.py --gl-all
  python run.py --validate
  python run.py --health
  python run.py --setup   (runs init + hierarchy + rates in one shot)
"""

import sys
import argparse
from pathlib import Path

# Add project root to path so imports work
sys.path.insert(0, str(Path(__file__).parent))

from scripts.pipeline import (
    setup_logging, init_platform, run_hierarchy,
    run_rates, run_ad50, run_gl, run_all_gl,
    run_validation, health_check
)


def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Finance Platform — Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py --setup
  python run.py --ad50 data/AD50_Jan.xlsx
  python run.py --ad50 data/AD50_Feb.xlsx --dry-run
  python run.py --gl-all
  python run.py --validate
  python run.py --health
        """
    )

    parser.add_argument("--init",      action="store_true",
                        help="Initialise DB and seed entities")
    parser.add_argument("--hierarchy", action="store_true",
                        help="Load BU hierarchy and AD50 line master")
    parser.add_argument("--rates",     action="store_true",
                        help="Load budget FX rates")
    parser.add_argument("--ad50",      type=str, metavar="FILE",
                        help="Load a SAC AD50 export file")
    parser.add_argument("--gl",        type=str, metavar="FILE",
                        help="Load a single GL transaction file")
    parser.add_argument("--gl-all",    action="store_true",
                        help="Load all GL files in data/jan_feb/")
    parser.add_argument("--validate",  action="store_true",
                        help="Run post-load validation checks")
    parser.add_argument("--health",    action="store_true",
                        help="Show DB table row counts")
    parser.add_argument("--setup",     action="store_true",
                        help="Full setup: init + hierarchy + rates")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Parse and validate only — no DB write")
    parser.add_argument("--skip-checks", action="store_true",
                        help="Skip pre-ingestion checks (not recommended)")
    parser.add_argument("--year",      type=int, default=2026,
                        help="Fiscal year for rates (default: 2026)")

    args = parser.parse_args()

    # No args → show help
    if len(sys.argv) == 1:
        parser.print_help()
        print("\n  Quick start:")
        print("    python run.py --setup")
        print("    python run.py --ad50 data/AD50_Jan.xlsx")
        print("    python run.py --gl-all")
        print("    python run.py --validate")
        print()
        sys.exit(0)

    # --setup: full initialisation
    if args.setup:
        print("\n🚀 Running full platform setup...")
        init_platform()
        run_hierarchy()
        run_rates(fiscal_year=args.year)
        health_check()
        print("✅ Setup complete. Ready to load data.\n")
        return

    # Individual commands
    if args.init:
        init_platform()

    if args.hierarchy:
        run_hierarchy()

    if args.rates:
        run_rates(fiscal_year=args.year)

    if args.ad50:
        result = run_ad50(
            args.ad50,
            dry_run=args.dry_run,
            skip_checks=args.skip_checks
        )
        if result.get("status") == "BLOCKED":
            print("\n❌ Load blocked — fix errors above and retry\n")
            sys.exit(1)

    if args.gl:
        result = run_gl(
            args.gl,
            dry_run=args.dry_run,
            skip_checks=args.skip_checks
        )
        if result.get("status") == "BLOCKED":
            print("\n❌ Load blocked — fix errors above and retry\n")
            sys.exit(1)

    if args.gl_all:
        result = run_all_gl(dry_run=args.dry_run)
        if result.get("errors", 0) > 0:
            print(f"\n⚠️  {result['errors']} file(s) had errors\n")

    if args.validate:
        run_validation()

    if args.health:
        health_check()


if __name__ == "__main__":
    main()
