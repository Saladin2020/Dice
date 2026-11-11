#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use dice_desktop::ui::DiceApp;

fn main() -> eframe::Result {
    let icon = load_app_icon();
    let mut viewport = egui::ViewportBuilder::default()
        .with_inner_size([1600.0, 860.0])
        .with_min_inner_size([1280.0, 720.0])
        .with_title("Dice — Target / Store / Destination / Transfer");
    if let Some(icon) = icon {
        viewport = viewport.with_icon(icon);
    }

    let options = eframe::NativeOptions {
        viewport,
        centered: true,
        ..Default::default()
    };

    eframe::run_native(
        "Dice",
        options,
        Box::new(|cc| Ok(Box::new(DiceApp::new(cc)))),
    )
}

fn load_app_icon() -> Option<egui::IconData> {
    let bytes = include_bytes!("../../assets/icon.png");
    let image = image::load_from_memory(bytes).ok()?.into_rgba8();
    let (width, height) = image.dimensions();
    Some(egui::IconData {
        rgba: image.into_raw(),
        width,
        height,
    })
}
