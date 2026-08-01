package snapshot

import "context"

func (c *Coordinator) ConfigureBackup(root string) error {
	backups, err := OpenBackupManager(root, c.manager)
	if err != nil {
		return err
	}
	c.backups = backups
	return nil
}

func (c *Coordinator) ExportBackup(ctx context.Context, req BackupRequest) (BackupRecord, error) {
	if c.backups == nil {
		return BackupRecord{}, ErrBackupUnavailable
	}
	return c.backups.Export(ctx, req)
}

func (c *Coordinator) GetBackup(backupID string) (BackupRecord, bool, error) {
	if c.backups == nil {
		return BackupRecord{}, false, ErrBackupUnavailable
	}
	record, ok := c.backups.Get(backupID)
	return record, ok, nil
}

func (c *Coordinator) ListBackups() ([]BackupRecord, error) {
	if c.backups == nil {
		return nil, ErrBackupUnavailable
	}
	return c.backups.List(), nil
}

func (c *Coordinator) ImportBackup(ctx context.Context, backupID string) (Record, error) {
	if c.backups == nil {
		return Record{}, ErrBackupUnavailable
	}
	return c.backups.Import(ctx, backupID, c.manager)
}
