{
  description = "Docker Swarm maintenance";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          skepr = pkgs.buildGoModule {
            pname = "skepr";
            version = self.shortRev or "dev";
            src = nixpkgs.lib.cleanSource ./.;

            vendorHash = "sha256-0cgcBSDxTAjgJJ7Fbz1fPY1tWxAUyahVjkSKAHluyvE=";
            subPackages = [ "cmd/skepr" ];

            nativeBuildInputs = [ pkgs.makeWrapper ];
            postInstall = ''
              wrapProgram "$out/bin/skepr" \
                --prefix PATH : ${nixpkgs.lib.makeBinPath [ pkgs.openssh ]}
            '';

            ldflags = [
              "-s"
              "-w"
            ];

            meta = {
              description = "Docker Swarm maintenance";
              homepage = "https://github.com/ebaldebo/skepr";
              mainProgram = "skepr";
            };
          };
        in
        {
          inherit skepr;
          default = skepr;
        }
      );

      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/skepr";
          meta.description = "Docker Swarm maintenance";
        };
      });

      checks = forAllSystems (system: {
        package = self.packages.${system}.default;
      });

      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixfmt);
    };
}
