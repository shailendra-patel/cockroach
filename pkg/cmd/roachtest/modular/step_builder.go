package modular

import "time"

// StepBuilder allows method chaining for building step sequences.
type StepBuilder struct {
	test  *Test
	stage *Stage
}

func newTestStep(hookID int, stepName string, fn stepFunc, opts ...StepOption) testStep {
	return testStep{
		StepProtocol: NewSingleStep(stepName, fn, opts...),
		hookID:       hookID,
	}
}

// Setup adds a setup step that runs before the test begins.
func (t *Test) Setup(stepName string, fn stepFunc, opts ...StepOption) {
	ts := newTestStep(t.nextHookID(), stepName, fn, opts...)

	// Create setup stage if it doesn't exist
	if t.setupStage == nil {
		t.setupStage = &Stage{
			name:   "setup",
			chains: make([]chain, 1),
		}
		// Start with an empty Chain
		t.setupStage.chains[0] = chain{}
	}

	// Add each setup step as a new stepGroup (sequential execution)
	t.setupStage.chains[0] = append(t.setupStage.chains[0], stepGroup{ts})
}

// AfterTest adds a step that runs after all test stages are complete.
func (t *Test) AfterTest(stepName string, fn stepFunc, opts ...StepOption) {
	ts := newTestStep(t.nextHookID(), stepName, fn, opts...)

	// Create after-test stage if it doesn't exist
	if t.afterTestStage == nil {
		t.afterTestStage = &Stage{
			name:   "after-test",
			chains: make([]chain, 1),
		}
		// Start with an empty Chain
		t.afterTestStage.chains[0] = chain{}
	}

	// Add each after-test step as a new stepGroup (sequential execution)
	t.afterTestStage.chains[0] = append(t.afterTestStage.chains[0], stepGroup{ts})
}

// NewStage creates a new stage for organizing test steps.
func (t *Test) NewStage(name string, opts ...StageOption) *Stage {
	stage := &Stage{
		name:               name,
		chains:             make([]chain, 0),
		maxStepConcurrency: t.options.defaultStepConcurrency,
	}

	for _, opt := range opts {
		opt(stage)
	}

	t.stages = append(t.stages, stage)
	return stage
}

// InStage adds a step to be executed in the specified stage.
func (t *Test) InStage(stage *Stage, stepName string, fn stepFunc, opts ...StepOption) *StepBuilder {
	ts := newTestStep(t.nextHookID(), stepName, fn, opts...)

	// Add step as a new Chain with a single stepGroup to the stage
	stage.chains = append(stage.chains, chain{stepGroup{ts}})

	return &StepBuilder{
		test:  t,
		stage: stage,
	}
}

// Then adds another step that runs after this one in sequence.
func (sb *StepBuilder) Then(stepName string, fn stepFunc, opts ...StepOption) *StepBuilder {
	ts := newTestStep(sb.test.nextHookID(), stepName, fn, opts...)

	// Add the step as a new stepGroup in the Chain
	if len(sb.stage.chains) == 0 {
		// Create a new Chain if none exists
		sb.stage.chains = append(sb.stage.chains, chain{stepGroup{ts}})
	} else {
		// Append to the last Chain as a new stepGroup
		lastChainIndex := len(sb.stage.chains) - 1
		sb.stage.chains[lastChainIndex] = append(sb.stage.chains[lastChainIndex], stepGroup{ts})
	}

	return sb
}

// And adds a step that can run in parallel with the previous step.
// All steps added via .And() will run in parallel within the same stepGroup.
func (sb *StepBuilder) And(stepName string, fn stepFunc, opts ...StepOption) *StepBuilder {
	ts := newTestStep(sb.test.nextHookID(), stepName, fn, opts...)

	if len(sb.stage.chains) == 0 {
		panic("no Chain found to add an And() step to")
	}

	lastChainIndex := len(sb.stage.chains) - 1
	lastChain := sb.stage.chains[lastChainIndex]
	if len(lastChain) == 0 {
		panic("no step group found to add an And() step to")
	}
	// Add to the last stepGroup
	lastStepGroupIndex := len(lastChain) - 1
	sb.stage.chains[lastChainIndex][lastStepGroupIndex] = append(lastChain[lastStepGroupIndex], ts)

	return sb
}

// OperationBuilder allows method chaining for building operations.
type OperationBuilder struct {
	Chain Chain
}

// NewOperation creates a new operation builder with the given name.
func NewOperation(stepName string, fn stepFunc, opts ...StepOption) *OperationBuilder {
	step := newTestStep(0 /* hookID */, stepName, fn, opts...)
	ob := &OperationBuilder{
		Chain: Chain{stepGroup{step}},
	}
	return ob
}

// Then adds another step that runs after the previous one in sequence.
func (ob *OperationBuilder) Then(stepName string, fn stepFunc, opts ...StepOption) *OperationBuilder {
	step := newTestStep(0 /* hookID */, stepName, fn, opts...)

	// Add step as a new stepGroup (sequential execution)
	ob.Chain = append(ob.Chain, stepGroup{step})
	return ob
}

// MaybeThen is like Then, but only adds the step if the conditional is true.
func (ob *OperationBuilder) MaybeThen(condition bool, stepName string, fn stepFunc, opts ...StepOption) *OperationBuilder {
	if !condition {
		return ob
	}
	step := newTestStep(0 /* hookID */, stepName, fn, opts...)

	// Add step as a new stepGroup (sequential execution)
	ob.Chain = append(ob.Chain, stepGroup{step})
	return ob
}

// And adds a step that can run in parallel with the previous step.
func (ob *OperationBuilder) And(stepName string, fn stepFunc, opts ...StepOption) *OperationBuilder {
	step := newTestStep(0 /* hookID */, stepName, fn, opts...)

	if len(ob.Chain) == 0 {
		panic("no step group found to add an And() step to")
	}

	// Add to the last stepGroup
	lastStepGroupIndex := len(ob.Chain) - 1
	ob.Chain[lastStepGroupIndex] = append(ob.Chain[lastStepGroupIndex], step)

	return ob
}

// MaybeAnd is like And, but only adds the step if the conditional is true.
func (ob *OperationBuilder) MaybeAnd(condition bool, stepName string, fn stepFunc, opts ...StepOption) *OperationBuilder {
	if !condition {
		return ob
	}
	step := newTestStep(0 /* hookID */, stepName, fn, opts...)

	if len(ob.Chain) == 0 {
		panic("no step group found to add an And() step to")
	}

	// Add to the last stepGroup
	lastStepGroupIndex := len(ob.Chain) - 1
	ob.Chain[lastStepGroupIndex] = append(ob.Chain[lastStepGroupIndex], step)

	return ob
}

// BuilderOperation wraps an OperationBuilder to implement the Operation interface.
// This allows OperationBuilder to be used wherever Operation is expected.
type BuilderOperation struct {
	name    string
	builder *OperationBuilder
}

// Build converts an OperationBuilder into an Operation with the given name.
// This is a convenience method for simple operations that don't need custom
// Precondition or Timeout implementations.
func (ob *OperationBuilder) Build(name string) Operation {
	return &BuilderOperation{
		name:    name,
		builder: ob,
	}
}

func (bo *BuilderOperation) Chain() Chain {
	return bo.builder.Chain
}

func (bo *BuilderOperation) Name() string {
	return bo.name
}

func (bo *BuilderOperation) Precondition() bool {
	return true
}

func (bo *BuilderOperation) Timeout() time.Duration {
	return 0 // No timeout by default
}

func (t *Test) AddOperation(stage *Stage, op Operation, opts ...StepOption) {
	var newChain chain
	for _, group := range op.Chain() {
		steps := make([]testStep, len(group))
		for i, step := range group {
			steps[i] = newTestStep(t.nextHookID(), step.Description(), step.Run, opts...)
		}
		newChain = append(newChain, steps)
	}

	stage.chains = append(stage.chains, newChain)
}
