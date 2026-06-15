param(
    [string]$ProductRoot = (Get-Location).Path,
    [string]$ArtifactDir = "",
    [string]$Namespace = "sw-block-phase42-gate",
    [string]$Kubectl = "kubectl"
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $ArtifactDir = Join-Path $ProductRoot "results\phase42-lifecycle-owner-admission-gate"
}
$Finalizer = "block.seaweedfs.com/swblockvolume-protection"
New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
$Summary = Join-Path $ArtifactDir "phase42-lifecycle-owner-admission-gate-summary.txt"
Set-Content -Path $Summary -Value ""

function Add-Summary([string]$Line) {
    Add-Content -Path $Summary -Value $Line
}

function Invoke-Capture {
    param([string]$Name, [string[]]$Args)
    $stdout = Join-Path $ArtifactDir "$Name.stdout.txt"
    $stderr = Join-Path $ArtifactDir "$Name.stderr.txt"
    & $Kubectl @Args >$stdout 2>$stderr
    return $LASTEXITCODE
}

function Expect-Success {
    param([string]$Name, [string[]]$Args)
    $code = Invoke-Capture -Name $Name -Args $Args
    if ($code -ne 0) {
        Get-Content (Join-Path $ArtifactDir "$Name.stderr.txt") -ErrorAction SilentlyContinue | Write-Error
        throw "expected success for $Name"
    }
}

function Expect-Failure {
    param([string]$Name, [string[]]$Args)
    $code = Invoke-Capture -Name $Name -Args $Args
    if ($code -eq 0) {
        Get-Content (Join-Path $ArtifactDir "$Name.stdout.txt") -ErrorAction SilentlyContinue | Write-Error
        throw "expected failure for $Name"
    }
}

function Cleanup {
    try { & $Kubectl delete validatingadmissionpolicybinding sw-block-phase42-finalizer-only --ignore-not-found >$null 2>$null } catch {}
    try { & $Kubectl delete validatingadmissionpolicy sw-block-phase42-finalizer-only --ignore-not-found >$null 2>$null } catch {}
    try { & $Kubectl delete clusterrolebinding sw-block-phase42-operator-status sw-block-phase42-lifecycle-owner --ignore-not-found >$null 2>$null } catch {}
    try { & $Kubectl delete clusterrole sw-block-phase42-operator-status sw-block-phase42-lifecycle-owner --ignore-not-found >$null 2>$null } catch {}
    try { & $Kubectl delete namespace $Namespace --ignore-not-found >$null 2>$null } catch {}
}

try {
    Add-Summary "phase42_lifecycle_owner_admission_status=running"
    Add-Summary "harness=live_kubernetes_validating_admission_policy"

    if ((Invoke-Capture -Name "kubectl-api-versions" -Args @("api-versions")) -ne 0) {
        Add-Summary "phase42_lifecycle_owner_admission_status=blocked"
        Add-Summary "blocked_reason=kubernetes_api_unreachable"
        exit 2
    }
    if ((Invoke-Capture -Name "api-resources-admissionregistration" -Args @("api-resources", "--api-group=admissionregistration.k8s.io")) -ne 0) {
        Add-Summary "phase42_lifecycle_owner_admission_status=blocked"
        Add-Summary "blocked_reason=kubernetes_api_unreachable"
        exit 2
    }
    $resources = Get-Content (Join-Path $ArtifactDir "api-resources-admissionregistration.stdout.txt")
    if (-not ($resources -match "^validatingadmissionpolicies\s")) {
        Add-Summary "phase42_lifecycle_owner_admission_status=blocked"
        Add-Summary "blocked_reason=validating_admission_policy_unavailable"
        exit 2
    }

    Expect-Success "apply-crd" @("apply", "-f", (Join-Path $ProductRoot "charts\seaweed-block\crds\swblockvolumes.block.seaweedfs.com.yaml"))

    @"
apiVersion: v1
kind: Namespace
metadata:
  name: $Namespace
  labels:
    block.seaweedfs.com/phase42-gate: "true"
"@ | Set-Content -Path (Join-Path $ArtifactDir "namespace.yaml")
    Expect-Success "apply-namespace" @("apply", "-f", (Join-Path $ArtifactDir "namespace.yaml"))

    @"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sw-block-operator-status
  namespace: $Namespace
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sw-block-lifecycle-owner
  namespace: $Namespace
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase42-operator-status
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase42-lifecycle-owner
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase42-operator-status
subjects:
  - kind: ServiceAccount
    name: sw-block-operator-status
    namespace: $Namespace
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase42-operator-status
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase42-lifecycle-owner
subjects:
  - kind: ServiceAccount
    name: sw-block-lifecycle-owner
    namespace: $Namespace
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase42-lifecycle-owner
"@ | Set-Content -Path (Join-Path $ArtifactDir "rbac.yaml")
    Expect-Success "apply-rbac" @("apply", "-f", (Join-Path $ArtifactDir "rbac.yaml"))

    @"
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: sw-block-phase42-finalizer-only
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
      - apiGroups: ["block.seaweedfs.com"]
        apiVersions: ["v1alpha1"]
        operations: ["UPDATE"]
        resources: ["swblockvolumes"]
  validations:
    - expression: >-
        request.userInfo.username != 'system:serviceaccount:${Namespace}:sw-block-lifecycle-owner' ||
        (
          object.spec == oldObject.spec &&
          (has(object.status) == has(oldObject.status)) &&
          (!has(object.status) || object.status == oldObject.status) &&
          (has(object.metadata.labels) == has(oldObject.metadata.labels)) &&
          (!has(object.metadata.labels) || object.metadata.labels == oldObject.metadata.labels) &&
          (has(object.metadata.annotations) == has(oldObject.metadata.annotations)) &&
          (!has(object.metadata.annotations) || object.metadata.annotations == oldObject.metadata.annotations) &&
          (has(object.metadata.ownerReferences) == has(oldObject.metadata.ownerReferences)) &&
          (!has(object.metadata.ownerReferences) || object.metadata.ownerReferences == oldObject.metadata.ownerReferences) &&
          (!has(object.metadata.finalizers) ||
            (size(object.metadata.finalizers) <= 1 &&
             object.metadata.finalizers.all(f, f == '${Finalizer}'))) &&
          (!has(oldObject.metadata.finalizers) ||
            (size(oldObject.metadata.finalizers) <= 1 &&
             oldObject.metadata.finalizers.all(f, f == '${Finalizer}')))
        )
      message: lifecycle-owner may patch only the Seaweed Block finalizer
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: sw-block-phase42-finalizer-only
spec:
  policyName: sw-block-phase42-finalizer-only
  validationActions: [Deny]
  matchResources:
    namespaceSelector:
      matchLabels:
        block.seaweedfs.com/phase42-gate: "true"
"@ | Set-Content -Path (Join-Path $ArtifactDir "admission.yaml")
    Expect-Success "apply-admission" @("apply", "-f", (Join-Path $ArtifactDir "admission.yaml"))

    $ownerAs = "system:serviceaccount:${Namespace}:sw-block-lifecycle-owner"

    @"
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: phase42-a
  namespace: $Namespace
  labels:
    keep: "true"
  annotations:
    keep: "true"
spec:
  pvcName: phase42-a
  storageClass: sw-block
"@ | Set-Content -Path (Join-Path $ArtifactDir "volume.yaml")
    Expect-Success "apply-volume" @("apply", "-f", (Join-Path $ArtifactDir "volume.yaml"))

    @"
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: phase42-admission-probe
  namespace: $Namespace
  labels:
    probe: "initial"
spec:
  pvcName: phase42-admission-probe
  storageClass: sw-block
"@ | Set-Content -Path (Join-Path $ArtifactDir "admission-probe.yaml")
    Expect-Success "apply-admission-probe" @("apply", "-f", (Join-Path $ArtifactDir "admission-probe.yaml"))

    $propagated = $false
    foreach ($attempt in 1..30) {
        $patch = "{`"metadata`":{`"labels`":{`"probe`":`"denied-$attempt`"}}}"
        $code = Invoke-Capture -Name "admission-policy-propagation-$attempt" -Args @("patch", "swblockvolume", "phase42-admission-probe", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", $patch)
        if ($code -eq 0) {
            Start-Sleep -Seconds 1
            continue
        }
        $stderr = Get-Content (Join-Path $ArtifactDir "admission-policy-propagation-$attempt.stderr.txt") -ErrorAction SilentlyContinue
        if ($stderr -match "lifecycle-owner may patch only") {
            $propagated = $true
            Add-Summary "admission_policy_propagated=true"
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $propagated) {
        Add-Summary "admission_policy_propagated=false"
        throw "validating admission policy did not deny a known-bad lifecycle-owner patch in time"
    }

    $operatorAs = "system:serviceaccount:${Namespace}:sw-block-operator-status"

    Expect-Failure "operator-status-main-patch" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $operatorAs, "--type=merge", "-p", "{`"metadata`":{`"finalizers`":[`"$Finalizer`"]}}")
    Add-Summary "operator_status_main_patch_allowed=false"
    Expect-Success "lifecycle-owner-add-finalizer" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", "{`"metadata`":{`"finalizers`":[`"$Finalizer`"]}}")
    Add-Summary "lifecycle_owner_finalizer_add_allowed=true"
    Expect-Success "lifecycle-owner-remove-finalizer" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", '{"metadata":{"finalizers":[]}}')
    Add-Summary "lifecycle_owner_finalizer_remove_allowed=true"

    Expect-Failure "lifecycle-owner-spec-patch" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", '{"spec":{"pvcName":"changed"}}')
    Add-Summary "lifecycle_owner_spec_patch_allowed=false"
    Expect-Failure "lifecycle-owner-label-patch" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", '{"metadata":{"labels":{"changed":"true"}}}')
    Add-Summary "lifecycle_owner_label_patch_allowed=false"
    Expect-Failure "lifecycle-owner-foreign-finalizer" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", '{"metadata":{"finalizers":["example.com/foreign"]}}')
    Add-Summary "lifecycle_owner_foreign_finalizer_allowed=false"
    Expect-Failure "lifecycle-owner-mixed-patch" @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--type=merge", "-p", "{`"metadata`":{`"finalizers`":[`"$Finalizer`"]},`"spec`":{`"pvcName`":`"changed`"}}")
    Add-Summary "lifecycle_owner_mixed_patch_allowed=false"

    $code = Invoke-Capture -Name "finalizers-endpoint" -Args @("patch", "swblockvolume", "phase42-a", "-n", $Namespace, "--as", $ownerAs, "--subresource=finalizers", "--type=merge", "-p", "{`"metadata`":{`"finalizers`":[`"$Finalizer`"]}}")
    if ($code -eq 0) { throw "unexpected /finalizers subresource success" }
    Add-Summary "finalizers_endpoint_allowed=false"

    foreach ($resource in @("pods", "deployments", "persistentvolumeclaims", "persistentvolumes", "storageclasses", "secrets", "nodes", "csidrivers", "csinodes")) {
        $code = Invoke-Capture -Name "can-i-patch-$resource" -Args @("auth", "can-i", "patch", $resource, "--as", $ownerAs, "-n", $Namespace)
        if ($code -eq 0) { throw "unexpected patch permission for $resource" }
        Add-Summary "lifecycle_owner_${resource}_patch_allowed=false"
    }

    Expect-Success "final-object" @("get", "swblockvolume", "phase42-a", "-n", $Namespace, "-o", "yaml")
    Add-Summary "phase42_lifecycle_owner_admission_status=ok"
}
finally {
    Cleanup
}
