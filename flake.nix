{
  description = "proktor: Traktor broadcast proxy with now-playing API, relay and recorder";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      version = "0.1.0";
    in
    {
      packages = forAllSystems (pkgs: rec {
        proktor = pkgs.buildGoModule {
          pname = "proktor";
          inherit version;
          src = self;
          vendorHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
          subPackages = [ "cmd/proktor" ];
          env.CGO_ENABLED = 0;
          ldflags = [ "-s" "-w" "-X main.version=${version}" ];
          meta = {
            description = "Traktor broadcast proxy: now-playing API, Icecast relay and recorder";
            mainProgram = "proktor";
          };
        };
        default = proktor;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [ go_1_24 gopls gotools ffmpeg jq curl ];
        };
      });

      nixosModules.default = import ./nix/module.nix self;
      nixosModules.proktor = self.nixosModules.default;
    };
}
