{ pkgs, ... }: {
  packages = [
    # pkgs.go
    # pkgs.python311
    # pkgs.python311Packages.pip
    pkgs.nodejs_20
    pkgs.postgresql
    pkgs.firebase-tools
    # pkgs.nodePackages.nodemon
  ];
  # Sets environment variables in the workspace
  env = {
    DATABASE_URL = "postgresql://user:password@localhost:5432/mydatabase";
  };
  idx = {
    # Search for the extensions you want on https://open-vsx.org/ and use "publisher.id"
    extensions = [
      # "vscodevim.vim"
      "dbaeumer.vscode-eslint"
      "googlecloudtools.firebase-vscode-extension"
      "google.gemini-cli-vscode-ide-companion"
    ];
    # Enable previews
    previews = {
      enable = true;
      previews = {
        web = {
          # Example: run "npm run dev" with PORT set to IDX's defined port for previews,
          # and show it in IDX's web preview panel
          command = ["node" "index.js"];
          manager = "web";
        };
      };
    };
    # Workspace lifecycle hooks
    workspace = {
      # Runs when a workspace is first created
      onCreate = {
        # Example: install JS dependencies from NPM
        npm-install = "npm install";
        # Open editors for the following files by default, if they exist:
        default.openFiles = [ ".idx/dev.nix" "README.md" "index.js" ];
      };
      # Runs when the workspace is (re)started
      onStart = {
        start-app = "npm start";
      };
    };
  };
}
