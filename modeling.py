import os
import xgboost as xgb
import joblib
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import classification_report, confusion_matrix


def train_xgboost_classifier(X_train, X_test, y_train, y_test):
    print("Initializing XGBoost training process...")
    param_grid = {
        "n_estimators": [100, 150],
        "max_depth": [3, 4],
        "learning_rate": [0.05, 0.1],
        "subsample": [0.8, 0.9],
        "colsample_bytree": [0.8, 0.9],
        "min_child_weight": [1, 3],
        "gamma": [0, 0.1],
    }

    model = xgb.XGBClassifier(
        objective="binary:logistic",
        random_state=42,
        eval_metric="logloss",
    )

    search = RandomizedSearchCV(
        estimator=model,
        param_distributions=param_grid,
        n_iter=10,  # Reduced iterations
        cv=2,  # Fewer cross-validation folds
        scoring="accuracy",
        n_jobs=-1,
        random_state=42,
        verbose=1,
    )

    print("Training XGBoost classifier with hyperparameter tuning...")
    search.fit(X_train, y_train)

    for i, params in enumerate(search.cv_results_["params"]):
        print(
            f"Training iteration {i+1}/{len(search.cv_results_['params'])} completed."
        )

    best_model = search.best_estimator_

    print("\nBest parameters found:", search.best_params_)

    print("Evaluating model on test set...")
    y_pred = best_model.predict(X_test)
    print("\nClassification Report:\n", classification_report(y_test, y_pred))
    print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))

    print("Training completed successfully.")
    return best_model


def save_model(model, filepath="models/xgboost_image_classifier_2.pkl"):
    print(f"Saving trained model to {filepath}...")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    joblib.dump(model, filepath)
    print(f"Model saved successfully at {filepath}.")
