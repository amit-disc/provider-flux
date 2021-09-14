/*
Copyright 2020 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package install

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	// "gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/yaml"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	"github.com/amit-disc/provider-flux/apis/install/v1alpha1"
	apisv1alpha1 "github.com/amit-disc/provider-flux/apis/v1alpha1"
	"github.com/amit-disc/provider-flux/internal/clients"
	"github.com/fluxcd/flux2/pkg/manifestgen/install"

	goyaml "github.com/go-yaml/yaml"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	errNotMyType    = "managed resource is not a Install custom resource"
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errGetCreds     = "cannot get credentials"

	errNewClient     = "cannot create new Service"
	errGetInstall    = "cannot get object"
	errCreateInstall = "cannot create object"
	errApplyInstall  = "cannot apply object"
	errDeleteInstall = "cannot delete object"

	errNotKubernetesObject      = "managed resource is not an Object custom resource"
	errNewKubernetesClient      = "cannot create new Kubernetes client"
	errFailedToCreateRestConfig = "cannot create new rest config using provider secret"

	errGetLastApplied          = "cannot get last applied"
	errUnmarshalTemplate       = "cannot unmarshal template"
	errFailedToMarshalExisting = "cannot marshal existing resource"
)

// Setup adds a controller that reconciles Install managed resources.
func Setup(mgr ctrl.Manager, l logging.Logger, rl workqueue.RateLimiter) error {
	name := managed.ControllerName(v1alpha1.InstallGroupKind)

	o := controller.Options{
		RateLimiter: ratelimiter.NewDefaultManagedRateLimiter(rl),
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.InstallGroupVersionKind),
		managed.WithExternalConnecter(&connector{
			kube:            mgr.GetClient(),
			usage:           resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newRestConfigFn: clients.NewRestConfig,
			newKubeClientFn: clients.NewKubeClient}),
		managed.WithLogger(l.WithValues("controller", name)),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o).
		For(&v1alpha1.Install{}).
		Complete(r)
}

// A connector is expected to produce an ExternalClient when its Connect method
// is called.
type connector struct {
	kube            client.Client
	usage           resource.Tracker
	newServiceFn    func(creds []byte) (interface{}, error)
	newRestConfigFn func(kubeconfig []byte) (*rest.Config, error)
	newKubeClientFn func(config *rest.Config) (client.Client, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Install)

	if !ok {
		return nil, errors.New(errNotKubernetesObject)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &apisv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	var rc *rest.Config
	var err error
	cd := pc.Spec.Credentials

	if cd.Source == xpv1.CredentialsSourceInjectedIdentity {
		rc, err = rest.InClusterConfig()
		if err != nil {
			return nil, errors.Wrap(err, errFailedToCreateRestConfig)
		}
	} else {
		var kc []byte
		if kc, err = resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors); err != nil {
			return nil, errors.Wrap(err, errGetCreds)
		}

		if rc, err = c.newRestConfigFn(kc); err != nil {
			return nil, errors.Wrap(err, errFailedToCreateRestConfig)
		}
	}

	k, err := c.newKubeClientFn(rc)
	if err != nil {
		return nil, errors.Wrap(err, errNewKubernetesClient)
	}

	return &external{
		client: resource.ClientApplicator{
			Client:     k,
			Applicator: resource.NewAPIPatchingApplicator(k),
		},
	}, nil
}

type external struct {
	logger logging.Logger
	client resource.ClientApplicator
}

// These fmt statements should be removed in the real implementation.
func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Install)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotMyType)
	}

	// These fmt statements should be removed in the real implementation.
	fmt.Printf("Observing: %+v", cr)
	// desired, err := getDesired(cr)
	// if err != nil {
	// 	return managed.ExternalObservation{}, err
	// }

	return managed.ExternalObservation{
		// Return false when the external resource does not exist. This lets
		// the managed resource reconciler know that it needs to call Create to
		// (re)create the resource, or that it has successfully been deleted.
		ResourceExists: false,

		// Return false when the external resource exists, but it not up to date
		// with the desired managed resource state. This lets the managed
		// resource reconciler know that it needs to call Update.
		ResourceUpToDate: true,

		// Return any details that may be required to connect to the external
		// resource. These will be stored as the connection secret.
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Install)
	flux_manifests := generateManifests(cr.Spec.ForProvider.Version)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotMyType)
	}

	fmt.Printf("Creating: %+v", cr)
	// obj, err := getDesired(cr)
	split_yaml, _ := SplitYAML([]byte(flux_manifests))
	//if err != nil {
	//	return managed.ExternalCreation{}, err
	// }
	// fmt.Printf("Creating Object: %+v", obj)
	for _, s := range split_yaml {
		chanObj, _ := DecodeYAML(s)
		obj := <-chanObj
		meta.AddAnnotations(obj, map[string]string{
			v1.LastAppliedConfigAnnotation: flux_manifests,
		})
		if err := c.client.Create(ctx, obj); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, errCreateInstall)
		}
	}
	return managed.ExternalCreation{
		// Optionally return any details that may be required to connect to the
		// external resource. These will be stored as the connection secret.
		ConnectionDetails: managed.ConnectionDetails{},
	}, nil
}

// func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
// 	cr, ok := mg.(*v1alpha1.Install)
// 	flux_manifests := generateManifests(cr.Spec.ForProvider.Version)
// 	if !ok {
// 		fmt.Printf("Gone in loop")
// 		return managed.ExternalCreation{}, errors.New(errNotKubernetesObject)
// 	}

// 	c.logger.Debug("Creating", "resource", cr)
// 	obj, err := getDesired(cr)
// 	if err != nil {
// 		return managed.ExternalCreation{}, err
// 	}

// 	meta.AddAnnotations(obj, map[string]string{
// 		v1.LastAppliedConfigAnnotation: string(flux_manifests),
// 	})

// 	if err := c.client.Create(ctx, obj); err != nil {
// 		return managed.ExternalCreation{}, errors.Wrap(err, errCreateInstall)
// 	}

// 	cr.Status.SetConditions(xpv1.Available())
// 	return managed.ExternalCreation{}, setObserved(cr, obj)
// }

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Install)
	flux_manifests := generateManifests(cr.Spec.ForProvider.Version)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotKubernetesObject)
	}

	c.logger.Debug("Updating", "resource", cr)

	obj, err := getDesired(cr)
	if err != nil {
		return managed.ExternalUpdate{}, err
	}

	meta.AddAnnotations(obj, map[string]string{
		v1.LastAppliedConfigAnnotation: string(flux_manifests),
	})

	if err := c.client.Apply(ctx, obj); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errApplyInstall)
	}

	cr.Status.SetConditions(xpv1.Available())
	return managed.ExternalUpdate{}, setObserved(cr, obj)
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Install)
	if !ok {
		return errors.New(errNotKubernetesObject)
	}

	c.logger.Debug("Deleting", "resource", cr)
	obj, err := getDesired(cr)
	if err != nil {
		return err
	}

	return errors.Wrap(resource.IgnoreNotFound(c.client.Delete(ctx, obj)), errDeleteInstall)
}

func getLastApplied(obj *v1alpha1.Install, observed *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	lastApplied, ok := observed.GetAnnotations()[v1.LastAppliedConfigAnnotation]
	if !ok {
		return nil, nil
	}

	last := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(lastApplied), last); err != nil {
		return nil, errors.Wrap(err, errUnmarshalTemplate)
	}

	if last.GetName() == "" {
		last.SetName(obj.Name)
	}

	return last, nil
}

func getDesired(obj *v1alpha1.Install) (*unstructured.Unstructured, error) {
	flux_manifests := generateManifests(obj.Spec.ForProvider.Version)
	desired := &unstructured.Unstructured{}
	yaml.Unmarshal([]byte(flux_manifests), desired)
	//split_y, _ := SplitYAML([]byte(flux_manifests))
	//fmt.Printf("Desired manifests %+v", reflect.TypeOf(split_y))
	// for _, s := range split_y {
	// 	chanObj, _ := DecodeYAML(s)
	// 	obj := <-chanObj
	// 	fmt.Println(obj)
	// }
	// fmt.Printf(flux_manifests)
	// Unmarshal the YAML document into the unstructured object.
	// desired := &unstructured.Unstructured{}
	yaml.Unmarshal([]byte(flux_manifests), desired)
	fmt.Printf("Object Desc: %+v", desired)

	if desired.GetName() == "" {
		desired.SetName(obj.Name)
	}
	return desired, nil
}

func setObserved(obj *v1alpha1.Install, observed *unstructured.Unstructured) error {
	var err error
	return err
}

// func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
// 	cr, ok := mg.(*v1alpha1.Install)
// 	if !ok {
// 		return managed.ExternalObservation{}, errors.New(errNotKubernetesObject)
// 	}

// 	c.logger.Debug("Observing", "resource", cr)

// 	desired, err := getDesired(cr)
// 	if err != nil {
// 		return managed.ExternalObservation{}, err
// 	}

// 	observed := desired.DeepCopy()

// 	err = c.client.Get(ctx, types.NamespacedName{
// 		Namespace: observed.GetNamespace(),
// 		Name:      observed.GetName(),
// 	}, observed)

// 	if kerrors.IsNotFound(err) {
// 		return managed.ExternalObservation{ResourceExists: false}, nil
// 	}
// 	if err != nil {
// 		return managed.ExternalObservation{}, errors.Wrap(err, errGetInstall)
// 	}

// 	if err = setObserved(cr, observed); err != nil {
// 		return managed.ExternalObservation{}, err
// 	}

// 	var last *unstructured.Unstructured
// 	if last, err = getLastApplied(cr, observed); err != nil {
// 		return managed.ExternalObservation{}, errors.Wrap(err, errGetLastApplied)
// 	}
// 	if last == nil {
// 		return managed.ExternalObservation{
// 			ResourceExists:   true,
// 			ResourceUpToDate: false,
// 		}, nil
// 	}

// 	if equality.Semantic.DeepEqual(last, desired) {
// 		c.logger.Debug("Up to date!")
// 		return managed.ExternalObservation{
// 			ResourceExists:   true,
// 			ResourceUpToDate: true,
// 		}, nil
// 	}

// 	return managed.ExternalObservation{
// 		ResourceExists:   true,
// 		ResourceUpToDate: false,
// 	}, nil
// }

func generateManifests(version string) string {
	opt := install.MakeDefaultOptions()
	opt.Version = string(version)
	manifest, _ := install.Generate(opt, "")
	return manifest.Content
}

func SplitYAML(resources []byte) ([][]byte, error) {

	dec := goyaml.NewDecoder(bytes.NewReader(resources))

	var res [][]byte
	for {
		var value interface{}
		err := dec.Decode(&value)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		valueBytes, err := goyaml.Marshal(value)
		if err != nil {
			return nil, err
		}
		res = append(res, valueBytes)
	}
	return res, nil
}

func DecodeYAML(data []byte) (<-chan *unstructured.Unstructured, <-chan error) {

	var (
		chanErr        = make(chan error)
		chanObj        = make(chan *unstructured.Unstructured)
		multidocReader = utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	)

	go func() {
		defer close(chanErr)
		defer close(chanObj)

		// Iterate over the data until Read returns io.EOF. Every successful
		// read returns a complete YAML document.
		for {
			buf, err := multidocReader.Read()
			if err != nil {
				if err == io.EOF {
					return
				}
				chanErr <- errors.Wrap(err, "failed to read yaml data")
				return
			}

			// Do not use this YAML doc if it is unkind.
			var typeMeta runtime.TypeMeta
			if err := yaml.Unmarshal(buf, &typeMeta); err != nil {
				continue
			}
			if typeMeta.Kind == "" {
				continue
			}

			// Define the unstructured object into which the YAML document will be
			// unmarshaled.
			obj := &unstructured.Unstructured{
				Object: map[string]interface{}{},
			}

			// Unmarshal the YAML document into the unstructured object.
			if err := yaml.Unmarshal(buf, &obj.Object); err != nil {
				chanErr <- errors.Wrap(err, "failed to unmarshal yaml data")
				return
			}

			// Place the unstructured object into the channel.
			chanObj <- obj
		}
	}()

	return chanObj, chanErr
}
