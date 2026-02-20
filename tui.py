"""Module to create datapackages via TUI."""

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.message import Message
from textual.screen import Screen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    SelectionList,
)
from textual.widgets.selection_list import Selection

import builder


class ComponentSelected(Message):
    """Signal that a component is selected."""

    def __init__(self, component_name: str) -> None:
        """Init."""
        self.component_name = component_name
        super().__init__()


class AttributeSelectionScreen(Screen):
    """Screen to select attributes for given component."""

    def __init__(self, component_name: str, attributes: dict) -> None:
        """Init."""
        super().__init__()
        self.component_name = component_name
        self.attributes = attributes

    def compose(self) -> ComposeResult:
        """Set up screen."""
        yield Header()
        yield Container(
            Label(f"Adding Resource from Component: {self.component_name}"),
            Label("Enter Resource Name:"),
            Input(placeholder="resource_name", id="resource_name_input"),
            Label("Select Attributes to include:"),
            SelectionList(
                *[
                    Selection(name, name, initial_state=True)
                    for name in self.attributes
                ],
                id="attr_selection",
            ),
            Horizontal(
                Button("Cancel", id="cancel_btn"),
                Button("Add Resource", variant="primary", id="add_btn"),
            ),
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """React on button pressed."""
        if event.button.id == "cancel_btn":
            self.app.pop_screen()
        elif event.button.id == "add_btn":
            resource_name = self.query_one("#resource_name_input").value
            selected_attrs = self.query_one("#attr_selection").selected
            if resource_name and selected_attrs:
                self.dismiss((resource_name, selected_attrs))
            else:
                self.app.notify(
                    "Please provide a name and select at least one attribute.",
                )


class PackageNameScreen(Screen):
    """Screen to enter the name of the data package."""

    def compose(self) -> ComposeResult:
        """Compose the screen widgets."""
        yield Header()
        yield Container(
            Label("Enter Package Name:"),
            Input(placeholder="my-package", id="package_name_input"),
            Button("Next", variant="primary", id="next_btn"),
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events."""
        if event.button.id == "next_btn":
            name = self.query_one("#package_name_input").value
            if name:
                self.dismiss(name)


class DPBuilderApp(App):
    """Main application for building data packages."""

    CSS = """
    Container {
        padding: 1;
    }
    ListItem {
        padding: 1;
    }
    Horizontal {
        height: auto;
        margin-top: 1;
    }
    SelectionList {
        height: 10;
    }
    """

    def __init__(self) -> None:
        """Initialize the application."""
        super().__init__()
        self.builder: builder.PackageBuilder = None
        self.added_resources = []

    def on_mount(self) -> None:
        """Handle application mount event."""
        self.push_screen(PackageNameScreen(), self.set_package_name)

    def set_package_name(self, name: str) -> None:
        """Set the package name and initialize the builder."""
        if name:
            self.builder = builder.PackageBuilder(name)
            self.refresh_ui()

    def refresh_ui(self) -> None:
        """Refresh the user interface."""
        if self.builder:
            self.title = f"Data Package Builder: {self.builder.package_name}"

        # We need a main view
        self.recompose()
        self.update_component_list()

    def compose(self) -> ComposeResult:
        """Compose the main application layout."""
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Label("Available Components")
                yield ListView(id="component_list")
            with Vertical(id="main_content"):
                yield Label("Added Resources")
                yield ListView(id="resource_list")

        yield Horizontal(
            Button("Save Package", id="save_btn", variant="success"),
            Button("Exit", id="exit_btn", variant="error"),
        )
        yield Footer()

    def update_component_list(self) -> None:
        """Update the list of available components."""
        components = builder.get_available_components()
        list_view = self.query_one("#component_list", ListView)
        list_view.clear()
        for comp in components:
            list_view.append(ListItem(Label(comp), id=f"comp_{comp}"))

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Handle selection of a component from the list."""
        if event.list_view.id == "component_list":
            component_name = event.item.query_one(Label).render().plain
            self.start_add_resource(component_name)

    def start_add_resource(self, component_name: str) -> None:
        """Open the attribute selection screen for a component."""
        if not self.builder:
            return

        def handle_result(result) -> None:  # noqa: ANN001
            if result:
                resource_name, selected_attrs = result
                resource = builder.ElementResourceBuilder(
                    component_name,
                    resource_name,
                    selected_attrs,
                )
                self.builder.add_resource(resource)
                self.added_resources.append((resource_name, component_name))
                self.update_resource_list()

        component = builder.Component.from_name(component_name)

        self.push_screen(
            AttributeSelectionScreen(component_name, component.attributes),
            handle_result,
        )

    def update_resource_list(self) -> None:
        """Update the list of added resources."""
        list_view = self.query_one("#resource_list", ListView)
        list_view.clear()
        for res_name, comp_name in self.added_resources:
            list_view.append(ListItem(Label(f"{res_name} (from {comp_name})")))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events."""
        if event.button.id == "save_btn":
            if self.builder:
                self.builder.infer_sequences_from_resources()
                self.builder.infer_busses_from_resources()
                self.builder.save_package()
                self.notify(
                    f"Package saved to datapackages/{self.builder.package_name}",
                )
        elif event.button.id == "exit_btn":
            self.exit()


if __name__ == "__main__":
    app = DPBuilderApp()
    app.run()
