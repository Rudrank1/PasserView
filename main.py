from preprocessing import preprocess_data
from modeling import train_xgboost_classifier, save_model


def main():
    print("Starting image classification pipeline...")

    print("Step 1: Loading and preprocessing images...")
    X_train, X_test, y_train, y_test = preprocess_data(
        "images_dataset_2", img_size=(128, 128), augment_factor=3
    )

    print("Step 2: Training the XGBoost classifier...")
    model = train_xgboost_classifier(X_train, X_test, y_train, y_test)

    print("Step 3: Saving the trained model...")
    save_model(model)

    print("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
