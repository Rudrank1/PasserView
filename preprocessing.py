import os
import cv2
import numpy as np
import Augmentor
import albumentations as A
from sklearn.model_selection import train_test_split
from tqdm import tqdm

def augment_images(folder):
    print(f"Starting augmentation for {folder}...")
    p = Augmentor.Pipeline(folder)
    p.rotate(probability=0.7, max_left_rotation=10, max_right_rotation=10)
    p.flip_left_right(probability=0.5)
    p.zoom(probability=0.5, min_factor=1.1, max_factor=1.5)
    p.sample(100)
    print(f"Augmentation completed for {folder}.")

def create_augmentation_pipeline():
    """Create an augmentation pipeline using albumentations"""
    return A.Compose([
        A.HorizontalFlip(p=0.5),
        A.RandomBrightnessContrast(p=0.2),
        A.Rotate(limit=30, p=0.3),
        A.Resize(224, 224),
        A.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225],
        ),
    ])

def augment_image(image, transform):
    return transform(image=image)["image"]

def load_and_augment_images(folder, label, img_size=(128, 128), augment_factor=3):
    print(f"Loading and augmenting images from {folder}...")
    data, labels = [], []
    transform = create_augmentation_pipeline()
    file_list = [f for f in os.listdir(folder) if f.endswith(".jpg")]
    total_files = len(file_list)

    for idx, filename in enumerate(
        tqdm(file_list, desc=f"Processing {os.path.basename(folder)}")
    ):
        img_path = os.path.join(folder, filename)
        img = cv2.imread(img_path)
        if img is None:
            print(f"Warning: Skipping unreadable image {filename}")
            continue

        # Resize original image
        img = cv2.resize(img, img_size)
        data.append(img.flatten())
        labels.append(label)

        # Ensure augmented images are the same size
        for _ in range(augment_factor):
            aug_img = augment_image(img, transform)
            # Resize augmented image to match original size
            aug_img = cv2.resize(aug_img, img_size)
            data.append(aug_img.flatten())
            labels.append(label)

        if (idx + 1) % 10 == 0:
            print(f"Processed {idx + 1}/{total_files} images from {folder}...")

    print(f"Finished loading {len(data)} images from {folder}.")
    # Convert to numpy arrays ensuring all images have the same shape
    data = np.array(data, dtype=np.float32)
    labels = np.array(labels)
    return data, labels

def preprocess_data(root_folder, img_size=(128, 128), augment_factor=3):
    print("Starting data preprocessing...")

    memorable_folder = os.path.join(root_folder, "memorable")
    forgettable_folder = os.path.join(root_folder, "forgettable")

    print("Processing 'memorable' images...")
    data_memorable, labels_memorable = load_and_augment_images(
        memorable_folder, 1, img_size, augment_factor
    )
    print("Processing 'forgettable' images...")
    data_forgettable, labels_forgettable = load_and_augment_images(
        forgettable_folder, 0, img_size, augment_factor
    )

    print("Combining and normalizing dataset...")
    X = np.vstack((data_memorable, data_forgettable)) / 255.0
    y = np.hstack((labels_memorable, labels_forgettable))

    print("Splitting dataset into training and testing sets...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(
        f"Data preprocessing complete: {len(X_train)} training samples, {len(X_test)} testing samples."
    )
    return X_train, X_test, y_train, y_test
