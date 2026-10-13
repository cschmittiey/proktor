self:
{ config, lib, pkgs, ... }:

let
  cfg = config.services.proktor;
  toml = pkgs.formats.toml { };
  configFile = toml.generate "proktor.toml" cfg.settings;
in
{
  options.services.proktor = {
    enable = lib.mkEnableOption "proktor, the Traktor broadcast proxy";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "proktor.packages.\${system}.default";
      description = "The proktor package to run.";
    };

    settings = lib.mkOption {
      type = toml.type;
      default = { };
      example = lib.literalExpression ''
        {
          event_name = "801 something";
          ingest.mount = [ { path = "/live"; password = "traktor"; on_conflict = "takeover"; } ];
          metadata.delay_seconds = 30;
          recorder.enabled = true;
        }
      '';
      description = ''
        proktor configuration, rendered to TOML. See proktor.example.toml.
        Relative paths (recordings, state journal) resolve against
        /var/lib/proktor. Keep secrets out of here (it ends up in the Nix
        store): use {option}`services.proktor.environmentFile` with
        PROKTOR_HTTP_TOKEN, PROKTOR_UPSTREAM_0_PASSWORD, etc.
      '';
    };

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = "/run/secrets/proktor.env";
      description = "File with PROKTOR_* environment overrides, typically secrets.";
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [ "-set" "metadata.delay_seconds=45" ];
      description = "Extra command-line arguments for `proktor serve`.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the ingest (8000) and HTTP API (8080) ports, as configured.";
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.proktor = {
      description = "proktor Traktor broadcast proxy";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      serviceConfig = {
        ExecStart = lib.escapeShellArgs ([ (lib.getExe cfg.package) "serve" "-config" "${configFile}" ] ++ cfg.extraArgs);
        EnvironmentFile = lib.mkIf (cfg.environmentFile != null) cfg.environmentFile;
        DynamicUser = true;
        StateDirectory = "proktor";
        WorkingDirectory = "/var/lib/proktor";
        Restart = "always";
        RestartSec = 2;
        # Hardening.
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictAddressFamilies = [ "AF_INET" "AF_INET6" "AF_UNIX" ];
        RestrictNamespaces = true;
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        SystemCallArchitectures = "native";
      };
    };

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall (
      let
        port = addr: lib.toInt (lib.last (lib.splitString ":" addr));
      in
      [
        (port (cfg.settings.ingest.listen or "0.0.0.0:8000"))
        (port (cfg.settings.http.listen or "0.0.0.0:8080"))
      ]
    );
  };
}
