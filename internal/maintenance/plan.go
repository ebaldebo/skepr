package maintenance

import (
	"fmt"
	"net/url"
	"os"

	"github.com/BurntSushi/toml"
)

type Plan struct {
	Target struct {
		Hostname string `toml:"hostname"`
	} `toml:"target"`
	Swarm struct {
		Contexts  []string `toml:"contexts"`
		Endpoints []string `toml:"endpoints"`
	} `toml:"swarm"`
	Commands RunCommands `toml:"commands"`
}

func LoadPlan(path string) (Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, fmt.Errorf("read maintenance plan: %w", err)
	}
	var plan Plan
	metadata, err := toml.Decode(string(data), &plan)
	if err != nil {
		return Plan{}, fmt.Errorf("decode maintenance plan: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return Plan{}, fmt.Errorf("maintenance plan contains unknown key %s", undecoded[0])
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (p Plan) Validate() error {
	if p.Target.Hostname == "" {
		return fmt.Errorf("maintenance plan target.hostname is required")
	}
	if len(p.Commands.Update) == 0 || p.Commands.Update[0] == "" {
		return fmt.Errorf("maintenance plan commands.update must contain an executable")
	}
	for name, command := range map[string][]string{
		"pre": p.Commands.Pre, "update": p.Commands.Update, "verify": p.Commands.Verify,
	} {
		for _, argument := range command {
			if argument == "" {
				return fmt.Errorf("maintenance plan commands.%s contains an empty argument", name)
			}
		}
	}
	for _, contextName := range p.Swarm.Contexts {
		if contextName == "" {
			return fmt.Errorf("maintenance plan swarm.contexts contains an empty context name")
		}
	}
	for _, endpoint := range p.Swarm.Endpoints {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme != "ssh" || parsed.Host == "" {
			return fmt.Errorf("maintenance plan swarm.endpoints contains an unsupported endpoint; expected ssh:// endpoint")
		}
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return fmt.Errorf("maintenance plan swarm.endpoints must not contain passwords")
		}
	}
	return nil
}
