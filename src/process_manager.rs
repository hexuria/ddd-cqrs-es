use crate::command::CommandBus;
use std::fmt::{Display, Formatter};

#[cfg(feature = "async")]
use crate::async_api::AsyncCommandBus;

/// Event-driven policy that emits commands in response to events.
///
/// Process managers, also called sagas, should not mutate aggregate state
/// directly. They may keep their own state and should be designed for
/// idempotent event handling.
///
/// # Example
///
/// ```rust
/// use ddd_cqrs_es::ProcessManager;
///
/// #[derive(Clone)]
/// enum OrderEvent {
///     Placed { order_id: String },
/// }
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum ShippingCommand {
///     ShipOrder { order_id: String },
/// }
///
/// struct ShippingSaga;
///
/// impl ProcessManager<OrderEvent, ShippingCommand> for ShippingSaga {
///     type Error = std::convert::Infallible;
///
///     fn name(&self) -> &'static str { "shipping_saga" }
///
///     fn handle(&mut self, event: &OrderEvent) -> Result<Vec<ShippingCommand>, Self::Error> {
///         match event {
///             OrderEvent::Placed { order_id } => Ok(vec![
///                 ShippingCommand::ShipOrder { order_id: order_id.clone() }
///             ]),
///         }
///     }
/// }
///
/// let mut saga = ShippingSaga;
/// let commands = saga.handle(&OrderEvent::Placed { order_id: "order-123".to_string() }).unwrap();
/// assert_eq!(commands, vec![ShippingCommand::ShipOrder { order_id: "order-123".to_string() }]);
/// ```
pub trait ProcessManager<E, C> {
    /// Process manager error.
    type Error;

    /// Stable process manager name.
    fn name(&self) -> &'static str;

    /// Handles one event and returns commands to dispatch.
    fn handle(&mut self, event: &E) -> Result<Vec<C>, Self::Error>;
}

/// Error returned by process-manager runners.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProcessManagerRunnerError<ProcessError, CommandError> {
    /// Process manager event handling failed.
    ProcessManager(ProcessError),
    /// Command dispatch failed.
    CommandBus(CommandError),
}

impl<ProcessError, CommandError> Display for ProcessManagerRunnerError<ProcessError, CommandError>
where
    ProcessError: Display,
    CommandError: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessManagerRunnerError::ProcessManager(error) => Display::fmt(error, f),
            ProcessManagerRunnerError::CommandBus(error) => Display::fmt(error, f),
        }
    }
}

impl<ProcessError, CommandError> std::error::Error
    for ProcessManagerRunnerError<ProcessError, CommandError>
where
    ProcessError: std::error::Error + 'static,
    CommandError: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProcessManagerRunnerError::ProcessManager(error) => Some(error),
            ProcessManagerRunnerError::CommandBus(error) => Some(error),
        }
    }
}

/// Runs a process manager and dispatches emitted commands through a command bus.
#[derive(Clone, Debug)]
pub struct ProcessManagerRunner<P, B> {
    process_manager: P,
    command_bus: B,
}

impl<P, B> ProcessManagerRunner<P, B> {
    /// Creates a process-manager runner.
    pub fn new(process_manager: P, command_bus: B) -> Self {
        Self {
            process_manager,
            command_bus,
        }
    }

    /// Returns the wrapped process manager.
    pub fn process_manager(&self) -> &P {
        &self.process_manager
    }

    /// Returns the wrapped process manager mutably.
    pub fn process_manager_mut(&mut self) -> &mut P {
        &mut self.process_manager
    }

    /// Returns the command bus.
    pub fn command_bus(&self) -> &B {
        &self.command_bus
    }

    /// Returns the command bus mutably.
    pub fn command_bus_mut(&mut self) -> &mut B {
        &mut self.command_bus
    }

    /// Consumes the runner and returns the wrapped process manager and command bus.
    pub fn into_parts(self) -> (P, B) {
        (self.process_manager, self.command_bus)
    }
}

impl<P, B> ProcessManagerRunner<P, B> {
    /// Handles one event and dispatches all commands emitted by the process manager.
    #[expect(
        clippy::type_complexity,
        reason = "runner result type names both process-manager and command-bus errors"
    )]
    pub fn run<E, C>(
        &mut self,
        event: &E,
    ) -> Result<Vec<B::Output>, ProcessManagerRunnerError<P::Error, B::Error>>
    where
        P: ProcessManager<E, C>,
        B: CommandBus<C>,
    {
        let commands = self
            .process_manager
            .handle(event)
            .map_err(ProcessManagerRunnerError::ProcessManager)?;
        let mut outputs = Vec::with_capacity(commands.len());

        for command in commands {
            outputs.push(
                self.command_bus
                    .dispatch(command)
                    .map_err(ProcessManagerRunnerError::CommandBus)?,
            );
        }

        Ok(outputs)
    }
}

/// Async runner that dispatches emitted commands through an async command bus.
#[cfg(feature = "async")]
#[derive(Clone, Debug)]
pub struct AsyncProcessManagerRunner<P, B> {
    process_manager: P,
    command_bus: B,
}

#[cfg(feature = "async")]
impl<P, B> AsyncProcessManagerRunner<P, B> {
    /// Creates an async process-manager runner.
    pub fn new(process_manager: P, command_bus: B) -> Self {
        Self {
            process_manager,
            command_bus,
        }
    }

    /// Returns the wrapped process manager.
    pub fn process_manager(&self) -> &P {
        &self.process_manager
    }

    /// Returns the wrapped process manager mutably.
    pub fn process_manager_mut(&mut self) -> &mut P {
        &mut self.process_manager
    }

    /// Returns the command bus.
    pub fn command_bus(&self) -> &B {
        &self.command_bus
    }

    /// Returns the command bus mutably.
    pub fn command_bus_mut(&mut self) -> &mut B {
        &mut self.command_bus
    }

    /// Consumes the runner and returns the wrapped process manager and command bus.
    pub fn into_parts(self) -> (P, B) {
        (self.process_manager, self.command_bus)
    }
}

#[cfg(feature = "async")]
impl<P, B> AsyncProcessManagerRunner<P, B> {
    /// Handles one event and dispatches all commands emitted by the process manager.
    pub async fn run<E, C>(
        &mut self,
        event: &E,
    ) -> Result<Vec<B::Output>, ProcessManagerRunnerError<P::Error, B::Error>>
    where
        P: ProcessManager<E, C>,
        B: AsyncCommandBus<C>,
    {
        let commands = self
            .process_manager
            .handle(event)
            .map_err(ProcessManagerRunnerError::ProcessManager)?;
        let mut outputs = Vec::with_capacity(commands.len());

        for command in commands {
            outputs.push(
                self.command_bus
                    .dispatch(command)
                    .await
                    .map_err(ProcessManagerRunnerError::CommandBus)?,
            );
        }

        Ok(outputs)
    }
}
