package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

const csiNodeAppLabel = "sw-block-csi-node"

func (c *KubernetesStatusClient) EnrichNodeEvidence(ctx context.Context, namespace string, cluster ClusterEvidence) (ClusterEvidence, error) {
	nodes, err := c.readKubernetesNodes(ctx)
	if err != nil {
		return cluster, err
	}
	csiPods, err := c.readCSINodePods(ctx, namespace)
	if err != nil {
		return cluster, err
	}
	driverExists, err := c.csiDriverExists(ctx)
	if err != nil {
		return cluster, err
	}
	registered, err := c.readCSINodeDriverRegistrations(ctx)
	if err != nil {
		return cluster, err
	}
	return mergeLiveNodeEvidence(namespace, cluster, nodes, csiPods, driverExists, registered), nil
}

func (c *KubernetesStatusClient) readKubernetesNodes(ctx context.Context) ([]kubernetesNodeFact, error) {
	var list kubernetesNodeList
	if err := c.getJSON(ctx, "/api/v1/nodes", &list); err != nil {
		return nil, fmt.Errorf("read Kubernetes nodes: %w", err)
	}
	out := make([]kubernetesNodeFact, 0, len(list.Items))
	for _, item := range list.Items {
		fact := kubernetesNodeFact{
			Name:        item.Metadata.Name,
			InternalIP:  item.internalIP(),
			Ready:       item.ready(),
			Schedulable: !item.Spec.Unschedulable,
		}
		out = append(out, fact)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (c *KubernetesStatusClient) readCSINodePods(ctx context.Context, namespace string) (map[string]kubernetesPodFact, error) {
	var list kubernetesPodList
	path := "/api/v1/namespaces/" + pathEscape(namespace) + "/pods?labelSelector=app%3D" + pathEscape(csiNodeAppLabel)
	if err := c.getJSON(ctx, path, &list); err != nil {
		return nil, fmt.Errorf("read CSI node pods: %w", err)
	}
	out := map[string]kubernetesPodFact{}
	for _, item := range list.Items {
		nodeName := item.Spec.NodeName
		if nodeName == "" {
			continue
		}
		out[nodeName] = kubernetesPodFact{
			Name:          item.Metadata.Name,
			NodeName:      nodeName,
			Ready:         item.ready(),
			Namespace:     namespace,
			Images:        item.images(),
			MissingImages: item.imagePullMissingImages(),
		}
	}
	return out, nil
}

func (c *KubernetesStatusClient) csiDriverExists(ctx context.Context) (bool, error) {
	var driver kubernetesMetadataObject
	err := c.getJSON(ctx, "/apis/storage.k8s.io/v1/csidrivers/"+pathEscape(seaweedBlockCSIDriver), &driver)
	if err == nil {
		return true, nil
	}
	if isKubernetesNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("read CSIDriver: %w", err)
}

func (c *KubernetesStatusClient) readCSINodeDriverRegistrations(ctx context.Context) (map[string]bool, error) {
	var list kubernetesCSINodeList
	if err := c.getJSON(ctx, "/apis/storage.k8s.io/v1/csinodes", &list); err != nil {
		return nil, fmt.Errorf("read CSINodes: %w", err)
	}
	out := map[string]bool{}
	for _, item := range list.Items {
		for _, driver := range item.Spec.Drivers {
			if driver.Name == seaweedBlockCSIDriver {
				out[item.Metadata.Name] = true
				break
			}
		}
	}
	return out, nil
}

func (c *KubernetesStatusClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL(path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if c.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.BearerToken)
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return kubernetesAPIError{StatusCode: resp.StatusCode, Body: string(raw)}
}

func (c *KubernetesStatusClient) apiURL(path string) string {
	return stringsTrimRightSlash(c.BaseURL) + path
}

func mergeLiveNodeEvidence(namespace string, cluster ClusterEvidence, nodes []kubernetesNodeFact, csiPods map[string]kubernetesPodFact, driverExists bool, registered map[string]bool) ClusterEvidence {
	byKubernetesNode := map[string]int{}
	for i, node := range cluster.Nodes {
		if node.KubernetesNode != "" {
			byKubernetesNode[node.KubernetesNode] = i
		}
		if node.NodeName != "" {
			byKubernetesNode[node.NodeName] = i
		}
	}
	for _, fact := range nodes {
		idx, ok := byKubernetesNode[fact.Name]
		if !ok {
			cluster.Nodes = append(cluster.Nodes, NodeEvidence{NodeName: fact.Name, KubernetesNode: fact.Name})
			idx = len(cluster.Nodes) - 1
		}
		node := cluster.Nodes[idx]
		if node.NodeName == "" {
			node.NodeName = fact.Name
		}
		node.KubernetesNode = fact.Name
		node.InternalIP = firstNonEmptyString(node.InternalIP, fact.InternalIP)
		node.Ready = fact.Ready
		node.Schedulable = fact.Schedulable
		node.RequiredImages = appendUniqueStrings(node.RequiredImages, csiPods[fact.Name].Images...)
		node.MissingImages = appendUniqueStrings(node.MissingImages, csiPods[fact.Name].MissingImages...)
		node.Conditions = mergeNodeKubernetesConditions(node.Conditions, fact)
		node.Conditions = mergeNodeCSIEvidence(node.Conditions, namespace, fact.Name, csiPods[fact.Name], driverExists, registered[fact.Name])
		cluster.Nodes[idx] = node
	}
	sort.SliceStable(cluster.Nodes, func(i, j int) bool {
		return firstNonEmptyString(cluster.Nodes[i].KubernetesNode, cluster.Nodes[i].NodeName) <
			firstNonEmptyString(cluster.Nodes[j].KubernetesNode, cluster.Nodes[j].NodeName)
	})
	return cluster
}

func mergeNodeKubernetesConditions(conditions []ObservationCondition, fact kubernetesNodeFact) []ObservationCondition {
	if !fact.Ready {
		conditions = append(conditions, ObservationCondition{
			Type:         ConditionReady,
			Status:       "Unknown",
			Reason:       ReasonNodeNotReady,
			Severity:     "warning",
			Message:      "Kubernetes node Ready condition is not True",
			EvidenceRefs: []string{"kubernetes/node/" + fact.Name},
		})
	}
	if !fact.Schedulable {
		conditions = append(conditions, ObservationCondition{
			Type:         ConditionBlocked,
			Status:       "True",
			Reason:       ReasonNodeSchedulingDisabled,
			Severity:     "warning",
			Message:      "Kubernetes node is marked unschedulable",
			EvidenceRefs: []string{"kubernetes/node/" + fact.Name},
		})
	}
	return conditions
}

func mergeNodeCSIEvidence(conditions []ObservationCondition, namespace, nodeName string, pod kubernetesPodFact, driverExists, registered bool) []ObservationCondition {
	if len(pod.MissingImages) > 0 {
		conditions = append(conditions, ObservationCondition{
			Type:         ConditionReady,
			Status:       "False",
			Reason:       ReasonImageMissingOnNode,
			Severity:     "warning",
			Message:      "Seaweed Block CSI node pod has image-pull failure",
			EvidenceRefs: []string{"kubernetes/pod/" + firstNonEmptyString(pod.Namespace, namespace) + "/" + firstNonEmptyString(pod.Name, "missing")},
		})
		return conditions
	}
	if !driverExists || !registered {
		conditions = append(conditions, ObservationCondition{
			Type:         ConditionReady,
			Status:       "False",
			Reason:       ReasonCSIDriverNotRegistered,
			Severity:     "warning",
			Message:      "Seaweed Block CSI driver is not registered on this node",
			EvidenceRefs: []string{"kubernetes/csidriver/" + seaweedBlockCSIDriver, "kubernetes/csinode/" + nodeName},
		})
	}
	if !pod.Ready && len(pod.MissingImages) == 0 {
		conditions = append(conditions, ObservationCondition{
			Type:         ConditionReady,
			Status:       "False",
			Reason:       ReasonCSINodePodNotReady,
			Severity:     "warning",
			Message:      "Seaweed Block CSI node pod is not Ready on this node",
			EvidenceRefs: []string{"kubernetes/pod/" + firstNonEmptyString(pod.Namespace, namespace) + "/" + firstNonEmptyString(pod.Name, "missing")},
		})
	}
	return conditions
}

func isKubernetesNotFound(err error) bool {
	apiErr, ok := err.(kubernetesAPIError)
	return ok && apiErr.StatusCode == http.StatusNotFound
}

type kubernetesAPIError struct {
	StatusCode int
	Body       string
}

func (e kubernetesAPIError) Error() string {
	return fmt.Sprintf("http %d %s", e.StatusCode, e.Body)
}

type kubernetesNodeFact struct {
	Name        string
	InternalIP  string
	Ready       bool
	Schedulable bool
}

type kubernetesPodFact struct {
	Name          string
	Namespace     string
	NodeName      string
	Ready         bool
	Images        []string
	MissingImages []string
}

type kubernetesMetadataObject struct {
	Metadata kubernetesObjectMeta `json:"metadata"`
}

type kubernetesObjectMeta struct {
	Name string `json:"name"`
}

type kubernetesNodeList struct {
	Items []kubernetesNodeObject `json:"items"`
}

type kubernetesNodeObject struct {
	Metadata kubernetesObjectMeta `json:"metadata"`
	Spec     struct {
		Unschedulable bool `json:"unschedulable"`
	} `json:"spec"`
	Status struct {
		Addresses  []kubernetesNodeAddress   `json:"addresses"`
		Conditions []kubernetesNodeCondition `json:"conditions"`
	} `json:"status"`
}

type kubernetesNodeAddress struct {
	Type    string `json:"type"`
	Address string `json:"address"`
}

type kubernetesNodeCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

func (n kubernetesNodeObject) internalIP() string {
	for _, address := range n.Status.Addresses {
		if address.Type == "InternalIP" {
			return address.Address
		}
	}
	return ""
}

func (n kubernetesNodeObject) ready() bool {
	for _, condition := range n.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status == "True"
		}
	}
	return false
}

type kubernetesPodList struct {
	Items []kubernetesPodObject `json:"items"`
}

type kubernetesPodObject struct {
	Metadata kubernetesObjectMeta `json:"metadata"`
	Spec     struct {
		NodeName       string                `json:"nodeName"`
		Containers     []kubernetesContainer `json:"containers"`
		InitContainers []kubernetesContainer `json:"initContainers"`
	} `json:"spec"`
	Status struct {
		Conditions            []kubernetesPodCondition    `json:"conditions"`
		ContainerStatuses     []kubernetesContainerStatus `json:"containerStatuses"`
		InitContainerStatuses []kubernetesContainerStatus `json:"initContainerStatuses"`
	} `json:"status"`
}

type kubernetesContainer struct {
	Image string `json:"image"`
}

type kubernetesContainerStatus struct {
	Image string `json:"image"`
	State struct {
		Waiting *struct {
			Reason string `json:"reason"`
		} `json:"waiting"`
	} `json:"state"`
}

type kubernetesPodCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

func (p kubernetesPodObject) ready() bool {
	for _, condition := range p.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status == "True"
		}
	}
	return false
}

func (p kubernetesPodObject) images() []string {
	var images []string
	for _, container := range p.Spec.InitContainers {
		images = appendUniqueStrings(images, container.Image)
	}
	for _, container := range p.Spec.Containers {
		images = appendUniqueStrings(images, container.Image)
	}
	return images
}

func (p kubernetesPodObject) imagePullMissingImages() []string {
	var images []string
	for _, status := range p.Status.InitContainerStatuses {
		if imagePullWaitingReason(status.State.Waiting) {
			images = appendUniqueStrings(images, status.Image)
		}
	}
	for _, status := range p.Status.ContainerStatuses {
		if imagePullWaitingReason(status.State.Waiting) {
			images = appendUniqueStrings(images, status.Image)
		}
	}
	return images
}

func imagePullWaitingReason(waiting *struct {
	Reason string `json:"reason"`
}) bool {
	if waiting == nil {
		return false
	}
	return waiting.Reason == "ImagePullBackOff" || waiting.Reason == "ErrImagePull"
}

type kubernetesCSINodeList struct {
	Items []kubernetesCSINodeObject `json:"items"`
}

type kubernetesCSINodeObject struct {
	Metadata kubernetesObjectMeta `json:"metadata"`
	Spec     struct {
		Drivers []struct {
			Name string `json:"name"`
		} `json:"drivers"`
	} `json:"spec"`
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func appendUniqueStrings(values []string, add ...string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	for _, value := range add {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values
}

func stringsTrimRightSlash(value string) string {
	for len(value) > 0 && value[len(value)-1] == '/' {
		value = value[:len(value)-1]
	}
	return value
}
