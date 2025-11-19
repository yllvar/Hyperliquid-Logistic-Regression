import polars as pl
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, classification_report
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
import joblib
from pathlib import Path
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModelTrainer:
    def __init__(self, features_dir: str = "data/features", models_dir: str = "models"):
        self.features_dir = Path(features_dir)
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        self.scaler = StandardScaler()
        self.model = None

    def load_dataset(self, coin: str, date_str: str) -> pd.DataFrame:
        """Loads feature dataset."""
        # In a real scenario, we might load multiple days.
        # For now, load one file.
        file_path = self.features_dir / date_str / f"{coin}_features.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Convert to pandas for sklearn compatibility
        df = pl.read_parquet(file_path).to_pandas()
        
        # Sort by time to ensure chronological order
        df = df.sort_values("timestamp")
        return df

    def prepare_data(self, df: pd.DataFrame, target_col: str = "target"):
        """Splits data into X and y, and drops non-feature columns."""
        # Drop metadata columns
        drop_cols = ["timestamp", "coin", target_col]
        feature_cols = [c for c in df.columns if c not in drop_cols]
        
        X = df[feature_cols]
        y = df[target_col]
        
        return X, y

    def train(self, df: pd.DataFrame):
        """
        Trains the Logistic Regression model with chronological split.
        70% Train, 15% Val, 15% Test.
        """
        X, y = self.prepare_data(df)
        
        n = len(df)
        train_end = int(n * 0.70)
        val_end = int(n * 0.85)
        
        X_train, y_train = X.iloc[:train_end], y.iloc[:train_end]
        X_val, y_val = X.iloc[train_end:val_end], y.iloc[train_end:val_end]
        X_test, y_test = X.iloc[val_end:], y.iloc[val_end:]
        
        logger.info(f"Train size: {len(X_train)}, Val size: {len(X_val)}, Test size: {len(X_test)}")
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train Model
        # Simple GridSearch for C parameter
        # We use TimeSeriesSplit for CV within the training set if we wanted to be very strict,
        # but for this MVP we'll just fit on Train and evaluate on Val to pick best C?
        # Or just use GridSearchCV with TimeSeriesSplit on X_train.
        
        tscv = TimeSeriesSplit(n_splits=3)
        param_grid = {'C': [0.01, 0.1, 1, 10, 100]}
        
        grid = GridSearchCV(LogisticRegression(class_weight='balanced', solver='liblinear'), 
                            param_grid, cv=tscv, scoring='f1')
        
        logger.info("Starting Grid Search...")
        grid.fit(X_train_scaled, y_train)
        
        self.model = grid.best_estimator_
        logger.info(f"Best params: {grid.best_params_}")
        
        # Evaluate
        self.evaluate(X_val_scaled, y_val, "Validation")
        self.evaluate(X_test_scaled, y_test, "Test")
        
        # Save artifacts
        self.save_artifacts()

    def evaluate(self, X, y, set_name="Test"):
        y_pred = self.model.predict(X)
        y_prob = self.model.predict_proba(X)[:, 1]
        
        acc = accuracy_score(y, y_pred)
        prec = precision_score(y, y_pred, zero_division=0)
        rec = recall_score(y, y_pred, zero_division=0)
        f1 = f1_score(y, y_pred, zero_division=0)
        try:
            auc = roc_auc_score(y, y_prob)
        except:
            auc = 0.5 # Handle single class case
            
        logger.info(f"--- {set_name} Metrics ---")
        logger.info(f"Accuracy: {acc:.4f}")
        logger.info(f"Precision: {prec:.4f}")
        logger.info(f"Recall: {rec:.4f}")
        logger.info(f"F1 Score: {f1:.4f}")
        logger.info(f"ROC AUC: {auc:.4f}")
        
        return {"accuracy": acc, "precision": prec, "recall": rec, "f1": f1, "auc": auc}

    def save_artifacts(self):
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        model_path = self.models_dir / f"lr_model_{timestamp}.pkl"
        scaler_path = self.models_dir / f"scaler_{timestamp}.pkl"
        
        joblib.dump(self.model, model_path)
        joblib.dump(self.scaler, scaler_path)
        
        logger.info(f"Model saved to {model_path}")
        logger.info(f"Scaler saved to {scaler_path}")

