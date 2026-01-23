module sigs.k8s.io/karpenter/examples/karpenter

go 1.24.0

require (
	github.com/tgoodwin/kamera v0.0.0
	sigs.k8s.io/karpenter v0.0.0
	k8s.io/api v0.33.4
	k8s.io/apimachinery v0.33.4
	sigs.k8s.io/controller-runtime v0.19.0
)

replace github.com/tgoodwin/kamera => ../..
replace sigs.k8s.io/karpenter => /Users/tgoodwin/projects/karpenter
