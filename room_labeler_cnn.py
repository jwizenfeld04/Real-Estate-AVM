import os
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms, models
from PIL import Image
from typing import Dict, List, Tuple
from sklearn.model_selection import train_test_split
import logging
import time
from torch.optim.lr_scheduler import ReduceLROnPlateau

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

room_label_map = {
    "Bathroom": 0,
    "Kitchen": 1,
    "Living Room": 2,
    "Dining Room": 3,
    "Bedroom": 4,
    "Office": 5,
    "Closet": 6,
    "Basement": 7,
    "Stairs": 8,
    "Exterior": 9,
}

room_type_dir = "data/raw/kaggle/house_data"
room_type_labels = "kaggle_labels.csv"

# Maybe try different hyperparamaters
batch_size = 32
num_epochs = 10
learning_rate = 0.001


# --- Dataset Definition ---
class RoomDataset(Dataset):
    def __init__(self, data: pd.DataFrame, root_dir: str, label_map: Dict[str, int], transform=None):
        self.data = data
        self.root_dir = root_dir
        self.label_map = label_map
        self.transform = transform or transforms.ToTensor()

        # Validate and load image paths and labels
        self.image_paths, self.labels = self._load_image_paths_and_labels()

    def _get_pic_filename(self, filename: str) -> str:
        """Extracts the picture filename if it's prefixed with 'pic'."""
        return filename[filename.index("pic") :] if "pic" in filename else filename

    def _load_image_paths_and_labels(self) -> Tuple[List[str], List[int]]:
        """Loads and validates image paths and labels based on the source of the data."""
        image_paths, labels = [], []
        for _, row in self.data.iterrows():
            img_path = os.path.join(self.root_dir, str(row["image_name"]))
            if os.path.exists(img_path) and row["room_type"] in self.label_map:
                try:
                    with Image.open(img_path) as img:
                        img.verify()
                    image_paths.append(img_path)
                    labels.append(self.label_map[row["room_type"]])
                except (Image.UnidentifiedImageError, IOError):
                    logger.warning(f"Invalid image skipped: {img_path}")
        return image_paths, labels

    def __len__(self):
        return len(self.image_paths)

    def __getitem__(self, idx):
        """Fetches an image and its label."""
        img_path = self.image_paths[idx]
        label = self.labels[idx]
        try:
            image = Image.open(img_path).convert("RGB")
            if self.transform:
                image = self.transform(image)
            return image, label
        except Exception as e:
            logger.error(f"Failed to load image at {img_path}: {e}")
            raise


# --- DataLoader ---
def create_data_loader(data, root_dir, label_map, batch_size, transform=None):
    transform = transform or transforms.Compose(
        [
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    dataset = RoomDataset(data=data, root_dir=root_dir, label_map=label_map, transform=transform)
    return DataLoader(dataset, batch_size=batch_size, shuffle=True, num_workers=2, pin_memory=False)


# --- Split Dataset ---
def split_dataset(data: pd.DataFrame, test_size: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split the dataset into training and testing sets."""
    # Try previous version when there are more labels so stratify works
    train_data, test_data = train_test_split(data, test_size=test_size, stratify=data["room_type"], random_state=42)
    # train_data, test_data = train_test_split(data, test_size=test_size, random_state=42)
    return train_data, test_data


# --- Model Definition ---
class RoomClassifier(nn.Module):
    def __init__(self, num_classes):
        super(RoomClassifier, self).__init__()
        self.base_model = models.resnet18(weights="ResNet18_Weights.DEFAULT")
        self.base_model.fc = nn.Linear(self.base_model.fc.in_features, num_classes)

    def forward(self, x):
        return self.base_model(x)


# --- Training and Evaluation ---
def train_and_evaluate(model, train_loader, device, num_epochs, criterion, optimizer, scheduler):
    for epoch in range(num_epochs):
        start_time = time.time()
        model.train()
        running_loss = 0.0

        for images, labels in train_loader:
            images, labels = images.to(device), labels.to(device)
            optimizer.zero_grad()
            outputs = model(images)
            loss = criterion(outputs, labels)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            running_loss += loss.item()

        epoch_loss = running_loss / len(train_loader)
        logger.info(f"Epoch [{epoch+1}/{num_epochs}], Training Loss: {epoch_loss:.4f}")

        # Scheduler step
        scheduler.step(epoch_loss)

        epoch_duration = time.time() - start_time
        logger.info(f"Epoch [{epoch+1}/{num_epochs}] completed in {epoch_duration:.2f} seconds")


def test_model(model, test_loader, device):
    model.eval()
    correct = 0
    total = 0
    test_loss = 0.0

    with torch.no_grad():
        for images, labels in test_loader:
            images, labels = images.to(device), labels.to(device)
            outputs = model(images)
            loss = criterion(outputs, labels)
            test_loss += loss.item()

            _, predicted = torch.max(outputs, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()

    accuracy = 100 * correct / total
    avg_loss = test_loss / len(test_loader)
    logger.info(f"Test Loss: {avg_loss:.4f}, Test Accuracy: {accuracy:.2f}%")
    return accuracy


# --- Main Execution ---
if __name__ == "__main__":
    try:
        device = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")
        logger.info(f"Using device: {device}")

        # Load and preprocess data from CSV2
        data = pd.read_csv(room_type_labels)
        data = data.rename(columns={"property_id": "image_name", "room_type": "room_type"})

        # Split dataset into training and testing sets
        train_data, test_data = split_dataset(data)

        # Create DataLoaders for both datasets
        train_loader = create_data_loader(train_data, room_type_dir, room_label_map, batch_size)
        test_loader = create_data_loader(test_data, room_type_dir, room_label_map, batch_size)

        # Initialize model, optimizer, and scheduler
        model = RoomClassifier(num_classes=len(room_label_map)).to(device)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=learning_rate)
        scheduler = ReduceLROnPlateau(optimizer, mode="min", patience=3, verbose=True)

        logger.info(f"Model architecture:\n{model}")
        train_and_evaluate(model, train_loader, device, num_epochs, criterion, optimizer, scheduler)

        # Test the model
        test_accuracy = test_model(model, test_loader, device)
        logger.info(f"Final Test Accuracy: {test_accuracy:.2f}%")

        # Save the model
        torch.save(model.state_dict(), "room_classifier18.1.pth")
        logger.info("Training and testing complete. Model saved to room_classifier.pth")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
