###############################################################
# flake.nix — Python Data Science Environment
# Python 3.12 + uv + Jupyter + NumPy + Pandas + PostgreSQL + Docker
###############################################################
{
  description = "ETL Salud Colombia — Warehouse Dimensional";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      pythonEnv = pkgs.python312.withPackages (ps: with ps; [
        ipykernel
        pyzmq
        jupyter-client
        notebook
        ipywidgets
        numpy
        pandas
        matplotlib
        seaborn
        scikit-learn
        sqlalchemy
        psycopg2
        psycopg2-binary
        python-dotenv
        requests
        httpx
        pytest
        black
        flake8
      ]);
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pythonEnv
          pkgs.uv
          pkgs.postgresql_16
          pkgs.docker-compose
          pkgs.podman-compose
          pkgs.stdenv.cc.cc.lib
          pkgs.zlib
        ];

        shellHook = ''
          if [ ! -d .venv ]; then
            uv venv --system-site-packages
          fi
          source .venv/bin/activate

          export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.zlib ]}:$LD_LIBRARY_PATH"

          python -m ipykernel install --user --name=.venv --display-name "Python (uv .venv)" > /dev/null 2>&1

          echo ""
          echo "╔══════════════════════════════════════════════════════════╗"
          echo "║  ETL Salud Colombia — Warehouse Dimensional              ║"
          echo "╚══════════════════════════════════════════════════════════╝"
          echo ""
          echo "Python:  $(python --version)"
          echo "uv:      $(uv --version)"
          echo "psql:    $(psql --version | head -1)"
          echo ""
          echo "Comandos:"
          echo "  docker-compose up -d        Levantar infraestructura (Docker)"
          echo "  podman-compose up -d        Levantar infraestructura (Podman)"
          echo "  docker-compose run --rm etl Ejecutar pipeline ETL"
          echo "  ./run_tests.sh unit         Ejecutar tests unitarios"
          echo "  ./run_tests.sh all          Ejecutar todos los tests"
          echo "  jupyter lab                 JupyterLab"
          echo "  uv pip install <pkg>        Instalar paquete"
          echo ""
          echo "Stack: NumPy, Pandas, Matplotlib, Scikit-learn, SQLAlchemy, psycopg2"
          echo ""
        '';
      };
    };
}