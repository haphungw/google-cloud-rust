# Cloud Rust: LRO Tracing Implementation

| \#begin-approvals-addon-section Username Role Status Last change [coryan](http://teams/coryan) Approver 🟢 Approved May 6, 2026 [dbolduc](http://teams/dbolduc) Reviewer 🟡 Pending May 6, 2026 [suzmue](http://teams/suzmue) Reviewer 🟢 LGTM May 7, 2026      ![][image1] Approval Instructions: Please approve or LGTM through the [G3 Assist](https://goto.google.com/g3a-approvals-reviewing) sidebar. For more information, see [go/g3a-approvals-reviewing](https://goto.google.com/g3a-approvals-reviewing)  |
| ----- |

Author: [Ha Phung](mailto:haphung@google.com)  
Last updated: May 5, 2026  
Self-link: [go/cloud-rust:lro-tracing-implementation](http://goto.google.com/cloud-rust:lro-tracing-implementation) 

# Objective

Add LRO (Long-Running Operation) tracing to the `google-cloud-rust` to enable customers to diagnose and monitor long-running operations. 

# Overview

This implementation follows the parent-child nesting hierarchy as defined in [go/client-libraries:lro-tracing](http://goto.google.com/client-libraries:lro-tracing). Before finalization, LRO tracing is gated behind `google-cloud-unstable-tracing`. We instrument the `PollerImpl` struct and the `until_done` method in the `google-cloud-lro` crate.

# Background

The Rust client libraries use the `google_cloud_lro::Poller` trait and `.until_done()` to handle LRO automated polling.

Currently, LRO traces appear as disconnected spans, making it hard to see the relationship between the initial call, backoff periods, and poll attempts, especially when the operation fails.

# Detailed Design

## Modifying traits and implementations

To carry the Span context across async boundaries, we update `PollerImpl` to store the parent span. This allows the polling loop to run within the correct trace context.

```rust
struct PollerImpl<S, Q> {
   // ... existing fields ...
   // New field to carry the T2 span context
   lro_span: Option<tracing::Span>,
}
```

To extend existing pollers with options (such as tracing) in a flexible, non-breaking manner, we introduce a `PollerExt` extension trait that implements `.with_options(PollerOptions)` for any poller.

To future-proof the API and avoid adding more arguments in the future, we use a `PollerOptions` struct for the new parameters. To support construction across crate boundaries while maintaining forward compatibility (via `#[non_exhaustive]`), `PollerOptions` derives `Default`.

```rust
pub struct TracingDetails {
   pub method_name: &'static str,
}

#[derive(Default)]
#[non_exhaustive]
pub struct PollerOptions {
   pub tracing: Option<TracingDetails>,
}

pub trait PollerExt<ResponseType, MetadataType> {
    fn with_options(self, options: PollerOptions) -> impl Poller<ResponseType, MetadataType>;
}
```

## Runtime configuration

Following the established pattern in the Google Cloud Rust SDK, tracing is only enabled at runtime if the client was constructed using the `.with_tracing()` method on the builder (which sets `config.tracing = true`).

To support this, we add a method `get_poller_options` to the generated service trait:
```rust
fn get_poller_options(&self, options: &crate::RequestOptions, method_name: &'static str) -> google_cloud_lro::internal::PollerOptions {
    google_cloud_lro::internal::PollerOptions::default()
}
```
The default implementation returns `PollerOptions::default()`. The generated `tracing` decorator overrides this method to return `PollerOptions` with `TracingDetails` populated (containing the method name), effectively signalling that tracing is enabled.

The generated `poller()` method calls `get_poller_options` to determine if tracing should be active for the LRO, and applies `.with_options(poller_options)` on the poller.

## Instrumentation

We use the `tracing` crate for instrumentation, and `.instrument(span)` on async blocks or futures. This ensures that whenever the future is polled, the specified span becomes the current span, and any child spans created within that future are correctly nested.

### T2: LRO wait span

The T2 span represents the entire duration of the automated polling. To work around the limitation that `tracing::info_span!` requires string literals, we create a generic span `"lro_wait"` in the hand-written `google-cloud-lro` crate and override its name dynamically using the `"otel.name"` attribute, which is populated with the specific method name passed from the generated code.

#### In generated code (`builder.rs`)

The generator does not create the span directly. Instead, it populates the `method_name` in `poller_options` and calls the poller factory directly:

```rust
let mut poller_options = self.0.stub.get_poller_options(&self.0.options);
#[cfg(google_cloud_unstable_tracing)]
if let Some(ref mut details) = poller_options.tracing {
    details.method_name = "google_cloud_showcase_v1beta1::client::Echo::wait::until_done";
}

use google_cloud_lro::internal::PollerExt;
let poller = google_cloud_lro::internal::new_poller(
    polling_error_policy,
    polling_backoff_policy,
    start,
    query,
).with_options(poller_options);
```

#### In Hand-written Library (`ext.rs`)

The `PollerExt::with_options` implementation creates the T2 span using the generic `"lro_wait"` name and overrides it with `otel.name = details.method_name` if tracing is active, returning a `Tracing` decorated poller wrapped in an `Either` choice:

```rust
impl<ResponseType, MetadataType, T> PollerExt<ResponseType, MetadataType> for T
where
    T: Poller<ResponseType, MetadataType>,
    ResponseType: Send,
    MetadataType: Send,
{
    fn with_options(self, options: PollerOptions) -> impl Poller<ResponseType, MetadataType> {
        if let Some(t) = options.tracing {
            let span = tracing::info_span!(
                "LRO Wait",
                "gcp.rpc.method" = t.method_name,
                "gcp.longrunning.operation_name" = tracing::field::Empty
            );
            let traced = Tracing::new(self, span);
            return Either::Right(traced);
        }
        Either::Left(self)
    }
}
```

// In until_done()
async fn until_done(mut self) -> Result<ResponseType> {
    let span = self.longrunning_span.take().unwrap_or_else(|| tracing::Span::none());
    let fut = async move {
        // polling loop...
    };
    fut.instrument(span).await
}
```

#### Resumed polling fallback

For resumed polling where the specific method context is lost, `google-cloud-lro` will fall back to using `"google_longrunning::Operations::until_done"` as the method name, which is also set as the `otel.name` attribute on the generic `"lro_wait"` span.

### T5: Sleep span

The polling loop includes backoff sleep intervals. We wrap the `tokio::time::sleep` call in a span named `LRO Sleep` to visualize these wait periods as children of the T2 span, using standard ambient span nesting:

```rust
// Conditional implementation in `until_done`
let sleep_span = if self.longrunning_span.is_some() {
    Some(tracing::info_span!("LRO Sleep"))
} else {
    None
};

let sleep_fut = tokio::time::sleep(self.backoff_policy.wait_period(&state));

if let Some(span) = sleep_span {
    use tracing::Instrument as _;
    sleep_fut.instrument(span).await;
} else {
    sleep_fut.await;
}
```

### T3: Poll attempt span

T3 poll attempt spans represent the individual `GetOperation` RPC calls. Since `GetOperation` is a standard generated RPC, it is automatically instrumented via the generated tracing decorator (`tracing.rs`) using the `client_request_signals!` macro. We do not manually create a T3 span inside `PollerImpl::poll()`.

To populate LRO-specific attributes on this automatically generated T3 span (such as `gcp.longrunning.poll_attempt_count` and `gcp.longrunning.done`):

1. **State Propagation**: `PollerImpl` propagates the LRO state (attempt count, and whether it is the terminal poll) to the `query` closure.
2. **RequestOptions Extensions**: The `query` closure (generated in `builder.rs`) inserts this LRO state into the `RequestOptions` extensions.
3. **Recording in Telemetry Layer**: The generated `tracing.rs` decorator extracts the LRO state from `RequestOptions` and records the attributes on the T3 span returned by the `client_request_signals!` macro before it completes.

### T4: RPC span

T4 spans (network RPCs) are handled automatically by the existing transport-level tracing layer in the SDK. Since the `query` call is executed within the instrumented context of the T3 span, these T4 spans will naturally become children of the T3 poll attempt span.

## Attribute mapping

- `gcp.longrunning.operation_name`: Populated with Operation ID on T2 span once discovered.  
- `gcp.longrunning.done`: Propagated via `RequestOptions` extensions and recorded on the T3 span in `tracing.rs` when the LRO completes.
- `gcp.longrunning.poll_attempt_count`: Propagated via `RequestOptions` extensions and recorded on the T3 span in `tracing.rs` using the poller's `attempt_count`.
- `gcp.resource.destination.id`:  
  - The generated `start` closure (which makes the initial call) can access the T2 parent span via `tracing::Span::current()` and record `gcp.resource.destination.id` directly on it once derived from the request or response.  
  - This requires the T2 span to be created with the field declared as `tracing::field::Empty`.

## Impacted components

1. `google-cloud-lro` crate (hand-written):  
- Update `PollerImpl` and `new_poller` as described in the Detailed Design.  
2. `sidekick` templates:  
- The generator must be updated to pass the specific method name (e.g., `google_cloud_speech_v2::client::Speech::batch_recognize`) when calling `.with_options(poller_options)` on the poller in the generated client code.  
- If `gcp.resource.destination.id` is available at the time of calling the LRO, the generator should generate code to pass this identifier or populate it in the context before calling `.with_options(poller_options)`.

## Configuration and feature gating

### Runtime configuration

Following the established pattern in the Google Cloud Rust SDK, tracing is only enabled at runtime if the client was constructed using the `.with_tracing()` method on the builder (which sets `config.tracing = true`).

To support this, we add a method `get_poller_options` to the generated service trait:
```rust
fn get_poller_options(&self, options: &crate::RequestOptions, method_name: &'static str) -> google_cloud_lro::internal::PollerOptions {
    google_cloud_lro::internal::PollerOptions::default()
}
```
The default implementation returns `PollerOptions::default()`. The generated `tracing` decorator overrides this method to return `PollerOptions` with `TracingDetails` populated (containing the method name), effectively signalling that tracing is enabled.

The generated `poller()` method calls `get_poller_options` to determine if tracing should be active for the LRO, and applies `.with_options(poller_options)` on the poller.

### Compile-time feature gating

To avoid exposing unstable tracing designs to customers before they converge, all LRO tracing instrumentation will be gated behind the compiler flag `google_cloud_unstable_tracing`. This follows the established precedent for unstable tracing features (see [go/cloud-rust:feature-gating](http://goto.google.com/cloud-rust:feature-gating)).

This allows us to iterate on the design and implementation without committing to a stable API or behavior for observability in complex flows.

```rust
#[cfg(google_cloud_unstable_tracing)]
// Tracing specific code...
```

Using `--cfg` means that the `tracing` crate will become a required dependency for `google-cloud-lro`. 

# Alternatives considered

## Alternatives for runtime configuration

To strictly enforce that LRO tracing is only enabled when customers specify .with_tracing() on the client, we considered the following approaches to pass the configuration flag from the client to the poller.

We considered adding tracing_enabled: bool to RequestOptions in google-cloud-gax. While this would follow the standard pattern for request options, it implies that customers can toggle tracing per request, which violates the design that observability is a client-level switch.

We considered adding a method like fn is_tracing_enabled(&self) -> bool to the generated service trait. While this would allow avoiding modifications to gax, it would pollute the service traits with non-RPC methods that have nothing to do with the transport layer.

## Use a Decorator for `Poller` trait

We considered creating a `TracingPoller<P>` decorator that wraps any `Poller` and adds spans, similar to how regular RPCs use generated decorators.

However, if `PollerImpl::until_done` calls `self.poll()`, it calls its own method directly, bypassing the decorator's `poll()` implementation. This makes it difficult to create T3 spans for poll attempts without changing the implementation of `until_done`.

## Only instrumenting `until_done` without storing span in `PollerImpl`

We considered not adding `lro_span` to `PollerImpl` and only instrumenting `until_done`, which resulted in a simpler `PollerImpl` struct.

However, to get the specific method name on the T2 span, `PollerImpl` needs to know it. Since `until_done` takes no arguments, we must store either the name string or the span in `PollerImpl`. Thus, the complexity difference is minimal.

## Alternatives for runtime configuration

To strictly enforce that LRO tracing is only enabled when customers specify `.with_tracing()` on the client, we considered the following approaches to pass the configuration flag from the client to the poller.

### Explicit field in `RequestOptions`

We considered adding `tracing_enabled: bool` to `RequestOptions` in `google-cloud-gax`. While this would follow the standard pattern for request options, it implies that customers can toggle tracing per request, which violates the design that observability is a *client-level* switch.

We considered adding a method like `fn is_tracing_enabled(&self) -> bool` to the generated service trait. While this would allow avoiding modifications to `gax`, it would pollute the service traits with non-RPC methods that have nothing to do with the transport layer.

## Create T2 span in generated code using specific literals (Approach A)

We considered generating the specific span name as a literal directly in the generated `builder.rs` (e.g., `tracing::info_span!("Speech::batch_recognize::until_done")`). 

This would provide a cleaner experience for local stdout logging (e.g., using `tracing-subscriber::fmt`), as the actual method name would print directly as the span name instead of being buried in attributes. 

However, we rejected this because it increases the complexity of the generator templates (requiring wrapping poller creation in `info_span!` and `in_scope` blocks in `builder.rs`). We decided that the simplification of the generator templates in Approach B outweighs the minor degradation in local stdout log readability, especially since standard RPCs already use a similar generic naming pattern (`"client_request"`).

